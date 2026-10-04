"""Tests for the first-class Messages SSE adapter in ``surge_cluster``.

这些测试全部使用 127.0.0.1 上的临时回环服务器，不访问外部网络，也不使用任何
真实凭据。
"""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from surge_cluster import (
    AdapterFailure,
    DAGScheduler,
    HttpWorkerAdapter,
    NodeSpec,
    RunSpec,
    extract_numeric_answer,
)


class _FakeHandler(BaseHTTPRequestHandler):
    payload = b""
    response_status = 200
    response_type = "text/event-stream"
    received: dict[str, Any] | None = None
    request_count = 0

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        cls = self.__class__
        length = int(self.headers.get("content-length", "0"))
        cls.received = json.loads(self.rfile.read(length).decode("utf-8"))
        cls.request_count += 1
        self.send_response(cls.response_status)
        self.send_header("content-type", cls.response_type)
        self.send_header("content-length", str(len(cls.payload)))
        self.end_headers()
        self.wfile.write(cls.payload)

    def log_message(self, *_args: object) -> None:
        return


def _sse(
    *,
    answer: str = '{"answer":"93.5%"}',
    output_tokens: int = 75,
    input_tokens: int = 10,
    complete: bool = True,
    include_usage: bool = True,
    reverse_tail: bool = False,
) -> bytes:
    usage = (
        {"usage": {"input_tokens": input_tokens, "output_tokens": output_tokens}}
        if include_usage
        else {"type": "message_delta"}
    )
    frames = [
        'event: message_start\ndata: {"type":"message_start"}\n\n',
        'event: content_block_start\ndata: {"type":"content_block_start"}\n\n',
        f'event: content_block_delta\ndata: {json.dumps({"delta": {"type": "text_delta", "text": answer}})}\n\n',
        'event: content_block_stop\ndata: {"type":"content_block_stop"}\n\n',
        f'event: message_delta\ndata: {json.dumps(usage)}\n\n',
    ]
    if complete:
        frames.append('event: message_stop\ndata: {"type":"message_stop"}\n\n')
    if reverse_tail and complete:
        frames[-1], frames[-2] = frames[-2], frames[-1]
    return "".join(frames).encode("utf-8")


class HttpAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        _FakeHandler.payload = _sse()
        _FakeHandler.response_status = 200
        _FakeHandler.response_type = "text/event-stream"
        _FakeHandler.received = None
        _FakeHandler.request_count = 0
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/v1/messages"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def _adapter(self, **kwargs: Any) -> HttpWorkerAdapter:
        options: dict[str, Any] = {
            "timeout": 2.0,
            "answer_extractor": extract_numeric_answer,
            "warning": "unit test call",
        }
        options.update(kwargs)
        return HttpWorkerAdapter(self.url, "test-model", **options)

    def _run(self, adapter: HttpWorkerAdapter, *, max_tokens: int = 2000) -> tuple[Any, Path]:
        directory = Path(tempfile.mkdtemp(prefix="surge-http-adapter-"))
        run = RunSpec(
            id="adapter-run",
            budget_cost=10.0,
            max_nodes=1,
            max_workers=1,
            deadline_seconds=30.0,
            cost_per_1k_tokens=0.01,
        )
        node = NodeSpec(id="solver", prompt="solve", max_attempts=1, timeout_seconds=10.0, max_tokens=max_tokens)
        with DAGScheduler(directory / "run.sqlite3", max_workers=1) as scheduler:
            scheduler.submit(run, [node])
            result = scheduler.execute(adapter)
        return result, directory

    def _artifact_text(self, directory: Path) -> str:
        """读取该 run 的内容寻址 artifact 目录中的全部文本。"""
        root = directory / "run-artifacts"
        if not root.exists():
            return ""
        return "\n".join(path.read_text(encoding="utf-8") for path in sorted(root.rglob("*")) if path.is_file())

    def test_adapter_is_public_library_api(self) -> None:
        self.assertEqual(HttpWorkerAdapter.__module__, "surge_cluster.http_adapter")

    def test_successful_call_produces_valid_envelope_and_artifact(self) -> None:
        result, _ = self._run(self._adapter())
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(list(result.succeeded), ["solver"])
        self.assertEqual(_FakeHandler.received["stream"], True)

    def test_request_respects_node_output_limit(self) -> None:
        result, _ = self._run(self._adapter(), max_tokens=100)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(_FakeHandler.received["max_tokens"], 100)

    def test_request_respects_policy_ceiling_below_node_limit(self) -> None:
        result, _ = self._run(self._adapter(max_output_tokens=500), max_tokens=4000)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(_FakeHandler.received["max_tokens"], 500)

    def test_usage_drives_envelope_cost(self) -> None:
        adapter = self._adapter(cost_per_1k_tokens=0.02)
        result, _ = self._run(adapter)
        self.assertEqual(result.status, "succeeded")
        self.assertAlmostEqual(result.spent_cost, (10 + 75) / 1000.0 * 0.02, places=9)

    def test_artifact_records_response_without_prompt_text(self) -> None:
        marker = "UNIQUE-PROMPT-MARKER-8842"
        directory = Path(tempfile.mkdtemp(prefix="surge-http-adapter-"))
        adapter = self._adapter()
        run = RunSpec(id="privacy-run", budget_cost=10.0, max_nodes=1, max_workers=1, deadline_seconds=30.0)
        node = NodeSpec(id="solver", prompt=marker, max_attempts=1, timeout_seconds=10.0, max_tokens=2000)
        with DAGScheduler(directory / "run.sqlite3", max_workers=1) as scheduler:
            scheduler.submit(run, [node])
            result = scheduler.execute(adapter)
        self.assertEqual(result.status, "succeeded")
        contents = self._artifact_text(directory)
        self.assertIn("http_status", contents)
        self.assertNotIn(marker, contents)

    def test_cancelled_before_dispatch_never_calls_endpoint(self) -> None:
        adapter = self._adapter()
        node = NodeSpec(id="solver", prompt="solve")
        context = {"should_stop": lambda: True}
        with self.assertRaises(AdapterFailure) as caught:
            adapter.run(node, context)
        self.assertEqual(caught.exception.kind, "cancelled")
        self.assertEqual(_FakeHandler.request_count, 0)

    def test_rejected_heartbeat_fails_closed(self) -> None:
        adapter = self._adapter()
        node = NodeSpec(id="solver", prompt="solve", timeout_seconds=5.0)
        with self.assertRaises(AdapterFailure) as caught:
            adapter.run(node, {"heartbeat": lambda _lease: False})
        self.assertEqual(caught.exception.kind, "cancelled")
        self.assertEqual(_FakeHandler.request_count, 0)

    def test_oversized_response_is_summarized_not_truncated(self) -> None:
        long_answer = "42 " + "x" * 5000
        _FakeHandler.payload = _sse(answer=long_answer)
        adapter = self._adapter(max_artifact_bytes=200)
        result, directory = self._run(adapter)
        self.assertEqual(result.status, "succeeded")
        combined = self._artifact_text(directory)
        self.assertIn("artifact_truncated", combined)
        self.assertIn("raw_response_sha256", combined)
        self.assertNotIn(long_answer, combined)

    def test_missing_event_fails_closed(self) -> None:
        _FakeHandler.payload = _sse(complete=False)
        adapter = self._adapter()
        result, _ = self._run(adapter)
        self.assertEqual(result.status, "failed")
        self.assertEqual(adapter.calls[-1]["kind"], "sse")

    def test_event_order_fails_closed(self) -> None:
        _FakeHandler.payload = _sse(reverse_tail=True)
        adapter = self._adapter()
        result, _ = self._run(adapter)
        self.assertEqual(result.status, "failed")
        self.assertEqual(adapter.calls[-1]["kind"], "sse")

    def test_missing_usage_fails_closed(self) -> None:
        _FakeHandler.payload = _sse(include_usage=False)
        adapter = self._adapter()
        result, _ = self._run(adapter)
        self.assertEqual(result.status, "failed")
        self.assertEqual(adapter.calls[-1]["kind"], "usage")

    def test_output_above_request_fails_closed(self) -> None:
        _FakeHandler.payload = _sse(output_tokens=2001)
        adapter = self._adapter()
        result, _ = self._run(adapter)
        self.assertEqual(result.status, "failed")
        self.assertEqual(adapter.calls[-1]["kind"], "output-limit")

    def test_oversized_payload_fails_closed(self) -> None:
        _FakeHandler.payload = _sse() + b"x" * 4096
        adapter = self._adapter(max_response_bytes=1024)
        result, _ = self._run(adapter)
        self.assertEqual(result.status, "failed")
        self.assertEqual(adapter.calls[-1]["kind"], "response-size")

    def test_non_sse_content_type_fails_closed(self) -> None:
        _FakeHandler.response_type = "application/json"
        adapter = self._adapter()
        result, _ = self._run(adapter)
        self.assertEqual(result.status, "failed")
        self.assertEqual(adapter.calls[-1]["kind"], "content-type")

    def test_empty_output_fails_closed(self) -> None:
        _FakeHandler.payload = _sse(answer="")
        adapter = self._adapter()
        result, _ = self._run(adapter)
        self.assertEqual(result.status, "failed")
        self.assertEqual(adapter.calls[-1]["kind"], "empty-output")

    def test_unusable_answer_fails_closed(self) -> None:
        _FakeHandler.payload = _sse(answer="no numbers in this reply")
        adapter = self._adapter()
        result, _ = self._run(adapter)
        self.assertEqual(result.status, "failed")
        self.assertEqual(adapter.calls[-1]["kind"], "empty-answer")

    def test_http_error_is_classified_and_retryable(self) -> None:
        _FakeHandler.response_status = 503
        _FakeHandler.payload = b"upstream unavailable"
        adapter = self._adapter()
        result, _ = self._run(adapter)
        self.assertEqual(result.status, "failed")
        self.assertEqual(adapter.calls[-1]["kind"], "http")
        self.assertTrue(adapter.calls[-1]["retryable"])

    def test_calls_evidence_never_contains_prompt(self) -> None:
        adapter = self._adapter()
        node = NodeSpec(id="solver", prompt="SECRET-PROMPT-TEXT", max_tokens=2000, timeout_seconds=5.0)
        adapter.run(node, {"heartbeat": lambda _lease: True})
        self.assertEqual(adapter.calls[0]["node_id"], "solver")
        self.assertNotIn("SECRET-PROMPT-TEXT", json.dumps(adapter.calls))

    def test_invalid_configuration_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            HttpWorkerAdapter("ftp://example.invalid", "model")
        with self.assertRaises(ValueError):
            HttpWorkerAdapter(self.url, "")
        with self.assertRaises(ValueError):
            HttpWorkerAdapter(self.url, "model", timeout=0)
        with self.assertRaises(ValueError):
            HttpWorkerAdapter(self.url, "model", max_output_tokens=0)
        with self.assertRaises(ValueError):
            HttpWorkerAdapter(self.url, "model", cost_per_1k_tokens=-1)


if __name__ == "__main__":
    unittest.main()
