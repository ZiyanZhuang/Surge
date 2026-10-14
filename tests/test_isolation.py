"""Tests for process-level isolation: hard timeouts and strict concurrency.

这些测试只在本机创建子进程，不访问网络。测试模块内的 adapter 都定义在模块层，
因为 ``spawn`` 需要在子进程中重新导入它们。
"""

from __future__ import annotations

import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any, Mapping
from unittest import mock

from surge_cluster import (
    AdapterFailure,
    AdapterTimeout,
    DAGScheduler,
    IsolatedAdapter,
    LocalEchoAdapter,
    NodeSpec,
    RunSpec,
    WorkerResult,
)


class _SleepAdapter:
    """无视协作式取消的 worker，用于验证硬超时。"""

    def __init__(self, seconds: float):
        self.seconds = seconds

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        time.sleep(self.seconds)
        return WorkerResult(
            envelope={
                "answer": "late",
                "claims": [],
                "citations": [],
                "confidence": 0.0,
                "warnings": [],
                "usage": {"total_tokens": 1, "cost": 0.0},
            }
        )


class _FailingAdapter:
    def __init__(self, kind: str = "sse", message: str = "injected failure"):
        self.kind = kind
        self.message = message

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        raise AdapterFailure(self.message, kind=self.kind, retryable=False)


class _NonSerializableAdapter:
    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        return WorkerResult(envelope={"answer": "x", "bad": float("nan")})


class _CrashAdapter:
    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        os._exit(2)


class _PaidFailureAdapter:
    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        result = WorkerResult({"answer": "failed", "usage": {"total_tokens": 15, "cost": 0.25}})
        raise AdapterFailure("paid verification failed", kind="verification", result=result)


class _InvalidArtifactAdapter:
    def __init__(self, artifacts: Any):
        self.artifacts = artifacts

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        result = LocalEchoAdapter().run(node, context)
        return WorkerResult(result.envelope, self.artifacts)


class _ContextProbeAdapter:
    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        keys = sorted(context)
        return WorkerResult(
            envelope={
                "answer": ",".join(keys),
                "claims": [],
                "citations": [],
                "confidence": 0.0,
                "warnings": [],
                "usage": {"total_tokens": 1, "cost": 0.0},
            }
        )


class _FileStampAdapter:
    """把 start/end 时刻写入文件，用于证明子进程没有重叠执行。"""

    def __init__(self, path: str, seconds: float):
        self.path = path
        self.seconds = seconds

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(f"start {os.getpid()} {time.time()}\n")
            handle.flush()
            time.sleep(self.seconds)
            handle.write(f"end {os.getpid()} {time.time()}\n")
            handle.flush()
        return WorkerResult(
            envelope={
                "answer": "stamped",
                "claims": [],
                "citations": [],
                "confidence": 0.0,
                "warnings": [],
                "usage": {"total_tokens": 1, "cost": 0.0},
            }
        )


def _node(**overrides: Any) -> NodeSpec:
    options: dict[str, Any] = {"id": "worker", "prompt": "do work", "timeout_seconds": 30.0}
    options.update(overrides)
    return NodeSpec(**options)


class IsolationTests(unittest.TestCase):
    def test_abrupt_child_exit_is_a_structured_crash(self) -> None:
        isolated = IsolatedAdapter(_CrashAdapter())
        with self.assertRaises(AdapterFailure) as caught:
            isolated.run(_node(), {})
        self.assertEqual(caught.exception.kind, "crash")
        self.assertEqual(isolated.stats()["started"], 1)

    def test_broken_result_pipe_is_a_structured_crash(self) -> None:
        isolated = IsolatedAdapter(_CrashAdapter())
        conn = mock.Mock()
        conn.poll.side_effect = BrokenPipeError("worker pipe closed")
        with self.assertRaises(AdapterFailure) as caught:
            isolated._await_result(conn, mock.Mock(), mock.Mock(), None, 1)
        self.assertEqual(caught.exception.kind, "crash")

    def test_spawn_failure_closes_resources_and_does_not_count_a_start(self) -> None:
        isolated = IsolatedAdapter(LocalEchoAdapter())
        parent, child = isolated._ctx.Pipe(duplex=False)
        control = mock.Mock()
        process = mock.Mock()
        process.pid = None
        process.start.side_effect = TypeError("cannot pickle worker")
        process.join.side_effect = AssertionError("cannot join an unstarted process")
        with mock.patch.object(isolated._ctx, "Pipe", return_value=(parent, child)), mock.patch.object(isolated._ctx, "Queue", return_value=control), mock.patch.object(isolated._ctx, "Process", return_value=process):
            with self.assertRaises(AdapterFailure) as caught:
                isolated.run(_node(), {})
        self.assertEqual(caught.exception.kind, "spawn")
        self.assertTrue(parent.closed)
        self.assertTrue(child.closed)
        process.close.assert_called_once()
        process.join.assert_not_called()
        control.close.assert_called_once()
        self.assertEqual(isolated.stats()["started"], 0)

    def test_unreaped_process_prevents_further_worker_admission(self) -> None:
        for kill_error in (False, True):
            with self.subTest(kill_error=kill_error):
                isolated = IsolatedAdapter(LocalEchoAdapter(), hard_timeout_grace=0, kill_grace=0.01)
                process = mock.Mock()
                process.pid = 123
                process.is_alive.return_value = True
                if kill_error:
                    process.kill.side_effect = PermissionError("termination rejected")
                with mock.patch.object(isolated._ctx, "Process", return_value=process) as factory:
                    with self.assertRaises(AdapterFailure) as first:
                        isolated.run(_node(timeout_seconds=0.01), {})
                    self.assertEqual(first.exception.kind, "reap")
                    with self.assertRaises(AdapterFailure) as second:
                        isolated.run(_node(), {})
                    self.assertEqual(second.exception.kind, "reap")
                factory.assert_called_once()
                process.terminate.assert_called()
                process.kill.assert_called()
    def test_timeout_graces_require_finite_numbers(self) -> None:
        for field in ("hard_timeout_grace", "kill_grace"):
            for value in (True, float("nan"), float("inf"), float("-inf"), -1):
                with self.subTest(field=field, value=value):
                    with self.assertRaisesRegex(ValueError, field):
                        IsolatedAdapter(LocalEchoAdapter(), **{field: value})

    def test_result_round_trip_through_scheduler(self) -> None:
        isolated = IsolatedAdapter(LocalEchoAdapter(), max_processes=2)
        with tempfile.TemporaryDirectory() as directory:
            run = RunSpec(id="iso", budget_cost=1.0, max_nodes=1, max_workers=1, deadline_seconds=60.0)
            with DAGScheduler(Path(directory) / "run.sqlite3", max_workers=1) as scheduler:
                scheduler.submit(run, [_node()])
                result = scheduler.execute(isolated)
                snapshot = scheduler.snapshot("iso")
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(snapshot[0]["attempt_count"], 1)
        self.assertEqual(isolated.stats()["started"], 1)

    def test_hard_timeout_kills_uncooperative_worker(self) -> None:
        isolated = IsolatedAdapter(_SleepAdapter(30.0), hard_timeout_grace=0.5, kill_grace=1.0)
        started = time.monotonic()
        with self.assertRaises(AdapterTimeout):
            isolated.run(_node(timeout_seconds=0.5), {})
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 8.0, "hard timeout must not wait for the sleep to finish")
        self.assertEqual(isolated.stats()["timed_out"], 1)

    def test_inner_failure_keeps_its_kind(self) -> None:
        isolated = IsolatedAdapter(_FailingAdapter(kind="content-type"))
        with self.assertRaises(AdapterFailure) as caught:
            isolated.run(_node(), {})
        self.assertEqual(caught.exception.kind, "content-type")
        self.assertFalse(caught.exception.retryable)

    def test_inner_failure_preserves_its_completed_result(self) -> None:
        with self.assertRaises(AdapterFailure) as caught:
            IsolatedAdapter(_PaidFailureAdapter()).run(_node(), {})
        self.assertEqual(caught.exception.kind, "verification")
        self.assertEqual(caught.exception.result.envelope["usage"], {"total_tokens": 15, "cost": 0.25})

    def test_non_serializable_envelope_fails_inside_the_boundary(self) -> None:
        isolated = IsolatedAdapter(_NonSerializableAdapter())
        with self.assertRaises(AdapterFailure) as caught:
            isolated.run(_node(), {})
        self.assertEqual(caught.exception.kind, "ValueError")

    def test_isolation_preserves_artifact_type_rejections(self) -> None:
        for artifacts in ({123: "text"}, {"notes": 123}, []):
            for isolated in (False, True):
                with self.subTest(artifacts=artifacts, isolated=isolated):
                    inner = _InvalidArtifactAdapter(artifacts)
                    adapter = IsolatedAdapter(inner) if isolated else inner
                    with tempfile.TemporaryDirectory() as directory:
                        with DAGScheduler(Path(directory) / "run.sqlite3") as scheduler:
                            scheduler.submit(RunSpec("invalid-artifact"), [_node(max_attempts=1)])
                            result = scheduler.execute(adapter)
                            self.assertEqual(result.status, "failed")
                            self.assertEqual(scheduler.artifacts("invalid-artifact"), [])

    def test_control_callbacks_are_forwarded_to_the_parent(self) -> None:
        isolated = IsolatedAdapter(_ContextProbeAdapter())
        seen: dict[str, Any] = {}

        def heartbeat(lease: float = 1.0) -> bool:
            seen["heartbeat"] = lease
            return True

        node = _node()
        result = isolated.run(
            node,
            {
                "run_id": "r1",
                "attempt_id": "a1",
                "wave": 0,
                "route": {"profile_id": "p"},
                "should_stop": lambda: False,
            },
        )
        keys = str(result.envelope["answer"]).split(",")
        # 可序列化的上下文子集被转发，回调类上下文不进入子进程。
        self.assertIn("route", keys)
        self.assertIn("run_id", keys)
        self.assertIn("heartbeat", keys)
        self.assertIn("should_stop", keys)
        self.assertNotIn("read_artifact", keys)

    def test_cancellation_terminates_the_child(self) -> None:
        isolated = IsolatedAdapter(_SleepAdapter(30.0), kill_grace=1.0)
        stop = threading.Event()
        threading.Timer(0.4, stop.set).start()
        started = time.monotonic()
        with self.assertRaises(AdapterFailure) as caught:
            isolated.run(_node(timeout_seconds=60.0), {"should_stop": stop.is_set})
        self.assertEqual(caught.exception.kind, "cancelled")
        self.assertLess(time.monotonic() - started, 8.0)

    def test_max_processes_serializes_child_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            stamp = str(Path(directory) / "stamps.txt")
            isolated = IsolatedAdapter(_FileStampAdapter(stamp, 0.4), max_processes=1)
            nodes = [_node(id=f"n{i}") for i in range(2)]
            threads = [threading.Thread(target=isolated.run, args=(item, {})) for item in nodes]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=60)
            spans = []
            pending: dict[str, float] = {}
            for line in Path(stamp).read_text(encoding="utf-8").splitlines():
                kind, pid, moment = line.split()
                if kind == "start":
                    pending[pid] = float(moment)
                else:
                    spans.append((pending.pop(pid), float(moment)))
        self.assertEqual(len(spans), 2)
        spans.sort()
        self.assertLessEqual(spans[0][1], spans[1][0], "children overlapped despite max_processes=1")

    def test_invalid_configuration_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            IsolatedAdapter(object())
        with self.assertRaises(ValueError):
            IsolatedAdapter(LocalEchoAdapter(), max_processes=0)
        with self.assertRaises(ValueError):
            IsolatedAdapter(LocalEchoAdapter(), hard_timeout_grace=-1)
        with self.assertRaises(ValueError):
            IsolatedAdapter(LocalEchoAdapter(), kill_grace=0)
        with self.assertRaises(ValueError):
            IsolatedAdapter(LocalEchoAdapter(), start_method="not-a-method")


if __name__ == "__main__":
    unittest.main()
