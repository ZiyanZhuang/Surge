"""把任意 ``WorkerAdapter`` 放进独立子进程执行，提供硬超时与严格物理并发。

设计论证
--------
调度器只依赖 ``WorkerAdapter.run(node, context)``。把隔离实现为同样满足该协议的
装饰器，DAG、波次闸门、预算预留与结算、租约、重试、artifact 提交和失败持久化全部
原样继承；另起一个"隔离调度器"会复制这些规则，并让两处规则随时间漂移。

该层维持的不变量：

* **I1 并发上界**：同一实例同时存活的子进程数 <= ``max_processes``（BoundedSemaphore）；
  调度器仍按 ``max_workers`` 限制每个 run 的在飞调用数，两者取更严者。
* **I2 硬超时**：子进程越过 ``node.timeout_seconds + hard_timeout_grace`` 即被终止，
  不合作 worker 无法继续占用调度槽位；这条在线程路径上无法保证。
* **I3 可序列化结果**：返回值先在子进程内做 JSON 往返，父进程不会收到 NaN、不可
  JSON 化的 mapping 或自定义对象，从而把错误定位在隔离边界内。
* **I4 单一写入者**：只有父进程写 SQLite。子进程通过单向控制队列上报
  ``heartbeat``/``progress``，由父进程转交给调度器的公开方法。

已知边界
--------
* 控制队列是单向的，子进程的 ``heartbeat`` 无法等待父进程回答，因此它一律返回
  ``True``。授权仍由调度器内部的 CAS 决定：即使租约已被回收，迟到结果也不会提交。
  隔离路径上取消由"终止子进程"实现，而不是由 heartbeat 返回值实现。
* 跨进程只转发可序列化的上下文子集（run/attempt/wave/stage/progress/route/artifacts）；
  ``read_artifact`` 这类回调不进入子进程。需要读取已提交证据的 verifier 应包在
  隔离层之外，在父进程内运行。
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import queue as queue_module
import threading
import time
import traceback
from typing import Any, Mapping

from .core import AdapterFailure, NodeSpec, WorkerResult

DEFAULT_MAX_PROCESSES = 4
DEFAULT_HARD_TIMEOUT_GRACE = 5.0
DEFAULT_KILL_GRACE = 2.0
_POLL_INTERVAL = 0.05
_FORWARDED_CONTEXT_KEYS = ("run_id", "attempt_id", "wave", "stage", "progress", "route", "artifacts")


class AdapterTimeout(AdapterFailure):
    """子进程越过硬超时被终止；语义上可重试。"""

    def __init__(self, message: str, *, kind: str = "timeout", retryable: bool = True):
        super().__init__(message, kind=kind, retryable=retryable)


def _json_safe(value: Any) -> Any:
    """JSON 往返：既做类型归一化，也把 NaN/Infinity 变成显式异常。"""
    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))


def _child_entry(
    conn: Any,
    control: Any,
    stop_event: Any,
    adapter: Any,
    node: NodeSpec,
    forwarded: Mapping[str, Any],
) -> None:
    """子进程入口：执行内层 adapter，只把可序列化结果送回父进程。"""

    def report_progress(value: Any, detail: Any = None) -> None:
        try:
            control.put(("progress", float(value), None if detail is None else str(detail)))
        except Exception:
            pass

    def heartbeat(lease_seconds: Any = None) -> bool:
        # 单向通道：无法等待父进程应答，授权由调度器 CAS 决定，见模块文档。
        try:
            control.put(("heartbeat", lease_seconds))
        except Exception:
            pass
        return True

    context: dict[str, Any] = dict(forwarded)
    context.update(
        {
            "report_progress": report_progress,
            "heartbeat": heartbeat,
            "should_stop": stop_event.is_set,
            "isolated": True,
            "worker_pid": os.getpid(),
        }
    )
    try:
        result = adapter.run(node, context)
        artifacts = getattr(result, "artifacts", None) or {}
        payload: dict[str, Any] = {
            "ok": True,
            "envelope": _json_safe(result.envelope),
            "artifacts": {str(name): str(content) for name, content in dict(artifacts).items()},
        }
    except BaseException as exc:  # noqa: BLE001 - 子进程必须把任何失败变成数据
        payload = {
            "ok": False,
            "error": str(exc) or exc.__class__.__name__,
            "kind": str(getattr(exc, "kind", exc.__class__.__name__)),
            "retryable": bool(getattr(exc, "retryable", False)),
            "traceback_tail": traceback.format_exc(limit=6)[-2000:],
        }
    # 内层 adapter 的逐次调用证据留在子进程里，这里把它带回父进程，避免隔离
    # 让报告失去可审计的调用明细。
    calls = getattr(adapter, "calls", None)
    if isinstance(calls, list) and calls and isinstance(calls[-1], Mapping):
        try:
            payload["adapter_call"] = _json_safe(dict(calls[-1]))
        except (TypeError, ValueError):
            pass
    try:
        conn.send(payload)
    except (BrokenPipeError, OSError):
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass


class IsolatedAdapter:
    """``WorkerAdapter`` 装饰器：内层 adapter 在独立子进程中执行。

    典型组合是 ``IsolatedAdapter(HttpWorkerAdapter(...))``：模型调用获得硬超时和
    严格物理并发，而调度器、预算与证据语义保持原样。
    """

    def __init__(
        self,
        adapter: Any,
        *,
        max_processes: int = DEFAULT_MAX_PROCESSES,
        hard_timeout_grace: float = DEFAULT_HARD_TIMEOUT_GRACE,
        kill_grace: float = DEFAULT_KILL_GRACE,
        start_method: str = "spawn",
    ):
        if not callable(getattr(adapter, "run", None)):
            raise ValueError("adapter must expose run(node, context)")
        if isinstance(max_processes, bool) or not isinstance(max_processes, int) or max_processes < 1:
            raise ValueError("max_processes must be a positive integer")
        if not isinstance(hard_timeout_grace, (int, float)) or hard_timeout_grace < 0:
            raise ValueError("hard_timeout_grace must not be negative")
        if not isinstance(kill_grace, (int, float)) or kill_grace <= 0:
            raise ValueError("kill_grace must be positive")
        if start_method not in mp.get_all_start_methods():
            raise ValueError(f"unsupported start method: {start_method}")
        self.adapter = adapter
        self.max_processes = max_processes
        self.hard_timeout_grace = float(hard_timeout_grace)
        self.kill_grace = float(kill_grace)
        self.start_method = start_method
        self._ctx = mp.get_context(start_method)
        # I1：信号量按实例共享，复用同一实例即可获得进程级全局上限。
        self._slots = threading.BoundedSemaphore(max_processes)
        self._stats_lock = threading.Lock()
        self.started = 0
        self.timed_out = 0
        # 从子进程带回的逐次调用证据；字段与内层 adapter 的记录一致。
        self.calls: list[dict[str, Any]] = []

    def stats(self) -> dict[str, int]:
        with self._stats_lock:
            return {"started": self.started, "timed_out": self.timed_out, "max_processes": self.max_processes}

    def _note(self, field: str) -> None:
        with self._stats_lock:
            setattr(self, field, getattr(self, field) + 1)

    def _reap(self, process: Any) -> None:
        """terminate -> join -> kill -> join，确保不留下孤儿进程。

        幂等：超时/取消路径与 ``finally`` 都可能回收同一个 process 对象，
        对已 close 的对象再次操作只会抛 ``ValueError``，这里直接视为已完成。
        """
        try:
            if process.is_alive():
                process.terminate()
                process.join(self.kill_grace)
            if process.is_alive():
                process.kill()
                process.join(self.kill_grace)
            process.join(0.2)
            if not process.is_alive():
                process.close()
        except ValueError:
            return

    def _drain(self, control: Any, context: Mapping[str, Any], stop_event: Any) -> None:
        heartbeat = context.get("heartbeat") if isinstance(context, Mapping) else None
        progress = context.get("report_progress") if isinstance(context, Mapping) else None
        while True:
            try:
                message = control.get(timeout=0.1)
            except (queue_module.Empty, OSError, ValueError):
                if stop_event.is_set():
                    return
                continue
            if not isinstance(message, tuple) or not message:
                continue
            if message[0] == "heartbeat" and callable(heartbeat):
                try:
                    lease = message[1] if len(message) > 1 else None
                    if isinstance(lease, (int, float)) and not isinstance(lease, bool):
                        heartbeat(float(lease))
                    else:
                        heartbeat()
                except Exception:
                    pass
            elif message[0] == "progress" and callable(progress) and len(message) > 1:
                try:
                    progress(message[1], message[2] if len(message) > 2 else None)
                except Exception:
                    pass

    def _await_result(
        self,
        parent_conn: Any,
        process: Any,
        stop_event: Any,
        should_stop: Any,
        hard_timeout: float,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + hard_timeout
        while True:
            if parent_conn.poll(_POLL_INTERVAL):
                try:
                    payload = parent_conn.recv()
                except EOFError as exc:
                    raise AdapterFailure(
                        "isolated worker exited without sending a result",
                        kind="crash",
                        retryable=False,
                    ) from exc
                if not isinstance(payload, dict):
                    raise AdapterFailure("isolated worker returned a malformed payload", kind="crash")
                return payload
            if callable(should_stop) and should_stop():
                stop_event.set()
                self._reap(process)
                raise AdapterFailure("isolated worker cancelled", kind="cancelled", retryable=False)
            if time.monotonic() >= deadline:
                stop_event.set()
                self._reap(process)
                self._note("timed_out")
                raise AdapterTimeout(
                    f"isolated worker exceeded hard timeout {hard_timeout:g}s and was terminated"
                )

    def run(self, node: NodeSpec, context: Mapping[str, Any]) -> WorkerResult:
        should_stop = context.get("should_stop") if isinstance(context, Mapping) else None
        hard_timeout = float(node.timeout_seconds) + self.hard_timeout_grace
        with self._slots:  # I1
            if callable(should_stop) and should_stop():
                raise AdapterFailure("isolated adapter cancelled before spawn", kind="cancelled")
            forwarded = {
                key: context[key]
                for key in _FORWARDED_CONTEXT_KEYS
                if isinstance(context, Mapping) and key in context
            }
            parent_conn, child_conn = self._ctx.Pipe(duplex=False)
            control = self._ctx.Queue()
            stop_event = self._ctx.Event()
            process = self._ctx.Process(
                target=_child_entry,
                args=(child_conn, control, stop_event, self.adapter, node, forwarded),
                daemon=True,
            )
            self._note("started")
            process.start()
            child_conn.close()
            reader = threading.Thread(
                target=self._drain, args=(control, context, stop_event), daemon=True
            )
            reader.start()
            try:
                payload = self._await_result(parent_conn, process, stop_event, should_stop, hard_timeout)
            finally:
                stop_event.set()
                self._reap(process)
                reader.join(timeout=1.0)
                try:
                    parent_conn.close()
                except OSError:
                    pass
                try:
                    control.close()
                except (OSError, ValueError):
                    pass
            if not payload.get("ok"):
                record = payload.get("adapter_call")
                if isinstance(record, dict):
                    self.calls.append({**record, "node_id": record.get("node_id") or node.id})
                raise AdapterFailure(
                    str(payload.get("error") or "isolated worker failed"),
                    kind=str(payload.get("kind") or "isolated"),
                    retryable=bool(payload.get("retryable")),
                )
            record = payload.get("adapter_call")
            if isinstance(record, dict):
                self.calls.append({**record, "node_id": record.get("node_id") or node.id})
            return WorkerResult(
                envelope=payload["envelope"],
                artifacts=payload["artifacts"],
            )
