"""Tests for the first-class Messages SSE adapter in ``surge_cluster``.

这些测试全部使用 127.0.0.1 上的临时回环服务器，不访问外部网络，也不使用任何
真实凭据。
"""

from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import run_dag

from surge_cluster import (
    AdapterFailure,
    DAGScheduler,
    HttpWorkerAdapter,
    NodeSpec,
    RunSpec,
    extract_numeric_answer,
)


class _FakeHandler(BaseHTTPRequestHandler):
    delay = 0.0
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
        time.sleep(cls.delay)
        try:
            self.send_response(cls.response_status)
            self.send_header("content-type", cls.response_type)
            self.send_header("content-length", str(len(cls.payload)))
            self.end_headers()
            self.wfile.write(cls.payload)
        except OSError:
            pass

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
    def test_cli_rejects_unsafe_report_before_http_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            plan = Path(directory) / "plan.json"
            plan.write_text(json.dumps({"schema_version": 1, "run": {"id": "safe-output"}, "nodes": [{"id": "n", "prompt": "work"}]}), encoding="utf-8")
            original = plan.read_bytes()
            code, report, output = run_dag.run(["--plan", str(plan), "--output", str(plan), "--endpoint", self.url])
            self.assertEqual(code, 2)
            self.assertEqual(report["error_kind"], "usage")
            self.assertIsNone(output)
            self.assertEqual(plan.read_bytes(), original)
        self.assertEqual(_FakeHandler.request_count, 0)

    def setUp(self) -> None:
        _FakeHandler.delay = 0.0
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

    def test_default_answer_extractor_preserves_plain_text(self) -> None:
        _FakeHandler.payload = _sse(answer="Evidence collected successfully.")
        result = HttpWorkerAdapter(self.url, "model").run(NodeSpec("n", "work"), {})
        self.assertEqual(result.envelope["answer"], "Evidence collected successfully.")

    def test_numeric_answer_extraction_keeps_unicode_negative_signs(self) -> None:
        for text in ("\u221242", "the result is \u221242", '{"answer":"\u221242"}', '{"answer":"\\u221242"}'):
            with self.subTest(text=text):
                self.assertEqual(extract_numeric_answer(text), "-42")

    def test_explicit_falsey_answer_extractor_is_not_replaced(self) -> None:
        class EmptyExtractor:
            called = False

            def __bool__(self):
                return False

            def __call__(self, text):
                self.called = True
                return None

        extractor = EmptyExtractor()
        with self.assertRaises(AdapterFailure) as caught:
            self._adapter(answer_extractor=extractor).run(NodeSpec("n", "work"), {})
        self.assertEqual(caught.exception.kind, "empty-answer")
        self.assertTrue(extractor.called)

    def test_unicode_separators_inside_json_text_do_not_split_sse_lines(self) -> None:
        for separator in ("\u0085", "\u2028", "\u2029"):
            with self.subTest(separator=repr(separator)):
                answer = "42" + separator + "units"
                escaped = json.dumps(separator)[1:-1].encode()
                _FakeHandler.payload = _sse(answer=answer).replace(escaped, separator.encode())
                result = HttpWorkerAdapter(self.url, "model").run(NodeSpec("n", "work"), {})
                self.assertEqual(result.envelope["answer"], answer)

    def test_successful_call_produces_valid_envelope_and_artifact(self) -> None:
        result, _ = self._run(self._adapter())
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(list(result.succeeded), ["solver"])
        self.assertEqual(_FakeHandler.received["stream"], True)

    def test_request_respects_node_output_limit(self) -> None:
        result, _ = self._run(self._adapter(), max_tokens=100)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(_FakeHandler.received["max_tokens"], 100)

    def test_node_timeout_caps_the_http_socket_timeout(self) -> None:
        _FakeHandler.delay = 0.2
        adapter = self._adapter(timeout=2)
        with self.assertRaises(AdapterFailure) as caught:
            adapter.run(NodeSpec("n", "work", timeout_seconds=0.05), {})
        self.assertEqual(caught.exception.kind, "transport")
        self.assertEqual(adapter.calls[-1]["kind"], "transport")

    def test_request_respects_policy_ceiling_below_node_limit(self) -> None:
        result, _ = self._run(self._adapter(max_output_tokens=500), max_tokens=4000)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(_FakeHandler.received["max_tokens"], 500)

    def test_usage_drives_envelope_cost(self) -> None:
        adapter = self._adapter(cost_per_1k_tokens=0.02)
        result, _ = self._run(adapter)
        self.assertEqual(result.status, "succeeded")
        self.assertAlmostEqual(result.spent_cost, (10 + 75) / 1000.0 * 0.02, places=9)

    def test_cache_input_tokens_are_counted_from_start_or_delta(self) -> None:
        cache = {"cache_read_input_tokens": 100, "cache_creation_input_tokens": 20}
        for location in ("start", "delta"):
            with self.subTest(location=location):
                payload = _sse()
                if location == "start":
                    payload = payload.replace(b'{"type":"message_start"}', json.dumps({"type": "message_start", "message": {"usage": cache}}).encode())
                else:
                    payload = payload.replace(b'{"input_tokens": 10, "output_tokens": 75}', json.dumps({"input_tokens": 10, "output_tokens": 75, **cache}).encode())
                _FakeHandler.payload = payload
                adapter = self._adapter()
                result, _ = self._run(adapter)
                self.assertEqual(result.status, "succeeded")
                self.assertAlmostEqual(result.spent_cost, 205 / 1000 * adapter.cost_per_1k_tokens)
                self.assertEqual(adapter.calls[-1]["usage"], {"input_tokens": 10, "output_tokens": 75, **cache})

    def test_cache_usage_must_be_nonnegative_and_cumulative(self) -> None:
        for value in (-1, False, "20", None):
            with self.subTest(value=value):
                _FakeHandler.payload = _sse().replace(b'{"input_tokens": 10, "output_tokens": 75}', json.dumps({"input_tokens": 10, "output_tokens": 75, "cache_read_input_tokens": value}).encode())
                with self.assertRaises(AdapterFailure) as caught:
                    self._adapter().run(NodeSpec("n", "work"), {})
                self.assertEqual(caught.exception.kind, "usage")
        _FakeHandler.payload = _sse().replace(b'{"type":"message_start"}', b'{"type":"message_start","message":{"usage":{"cache_read_input_tokens":100}}}').replace(b'{"input_tokens": 10, "output_tokens": 75}', b'{"input_tokens":10,"output_tokens":75,"cache_read_input_tokens":99}')
        with self.assertRaises(AdapterFailure):
            self._adapter().run(NodeSpec("n", "work"), {})

    def test_completed_response_rejections_retain_known_cost(self) -> None:
        def broken_extractor(_text):
            raise ValueError("cannot extract answer")

        for mode in ("empty-output", "empty-answer", "output-limit", "extractor-error", "unicode"):
            with self.subTest(mode=mode):
                _FakeHandler.payload = _sse(answer="" if mode == "empty-output" else "\ud800" if mode == "unicode" else "42")
                options = {"cost_per_1k_tokens": 0.02}
                if mode == "empty-answer":
                    options["answer_extractor"] = lambda _text: None
                elif mode == "extractor-error":
                    options["answer_extractor"] = broken_extractor
                elif mode == "output-limit":
                    options["max_output_tokens"] = 50
                elif mode == "unicode":
                    options["answer_extractor"] = lambda text: text
                adapter = self._adapter(**options)
                result, directory = self._run(adapter)
                self.assertEqual(result.status, "failed")
                self.assertAlmostEqual(result.spent_cost, 85 / 1000 * 0.02)
                self.assertFalse(adapter.calls[-1]["ok"])
                with DAGScheduler(directory / "run.sqlite3") as scheduler:
                    self.assertEqual(scheduler.artifacts("adapter-run"), [])
                    self.assertAlmostEqual(scheduler._conn.execute("SELECT cost FROM attempts").fetchone()[0], result.spent_cost)

    def test_cancellation_after_response_carries_known_usage(self) -> None:
        checks = 0

        def should_stop():
            nonlocal checks
            checks += 1
            return checks > 1

        with self.assertRaises(AdapterFailure) as caught:
            self._adapter().run(NodeSpec("n", "work"), {"should_stop": should_stop})
        self.assertEqual(caught.exception.kind, "cancelled")
        self.assertIsNotNone(caught.exception.result)
        self.assertAlmostEqual(caught.exception.result.envelope["usage"]["cost"], 85 / 1000 * 0.01)

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

    def test_heartbeat_does_not_raise_a_subsecond_node_lease(self) -> None:
        leases = []
        self._adapter(timeout=2.0).run(
            NodeSpec("n", "work", timeout_seconds=0.05),
            {"heartbeat": lambda lease: leases.append(lease) or True},
        )
        self.assertEqual(leases, [0.05])

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

    def test_complete_empty_stream_keeps_reported_input_cost(self) -> None:
        _FakeHandler.payload = b"\n\n".join(part for part in _sse(output_tokens=0).split(b"\n\n") if not part.startswith(b"event: content_block_delta\n"))
        adapter = self._adapter()
        result, _ = self._run(adapter)
        self.assertEqual(result.status, "failed")
        self.assertEqual(adapter.calls[-1]["kind"], "sse")
        self.assertAlmostEqual(result.spent_cost, 10 / 1000 * 0.01)

    def test_misordered_empty_stream_does_not_claim_complete_usage(self) -> None:
        parts = [part for part in _sse(output_tokens=0).split(b"\n\n") if not part.startswith(b"event: content_block_delta\n")]
        delta_index = next(index for index, part in enumerate(parts) if part.startswith(b"event: message_delta\n"))
        parts[0], parts[delta_index] = parts[delta_index], parts[0]
        _FakeHandler.payload = b"\n\n".join(parts)
        with self.assertRaises(AdapterFailure) as caught:
            self._adapter().run(NodeSpec("n", "work"), {})
        self.assertEqual(caught.exception.kind, "sse")
        self.assertIsNone(caught.exception.result)

    def test_malformed_sse_frames_fail_closed(self) -> None:
        payloads = {
            "invalid-json": _sse().replace(b'{"type":"message_start"}', b'not-json'),
            "invalid-text": _sse().replace(b'"text": "{\\\"answer\\\":\\\"93.5%\\\"}"', b'"text": 42'),
            "error-event": _sse() + b'event: error\ndata: {"type":"error","error":{"message":"failed"}}\n\n',
            "late-text": _sse() + b'event: content_block_delta\ndata: {"delta":{"type":"text_delta","text":"late"}}\n\n',
            "duplicate-stop": _sse() + b'event: message_stop\ndata: {"type":"message_stop"}\n\n',
            "invalid-utf8": _sse().replace(b'93.5%', b'93.5%\xff'),
        }
        for name, payload in payloads.items():
            with self.subTest(frame=name):
                _FakeHandler.payload = payload
                adapter = self._adapter()
                with self.assertRaises(AdapterFailure) as caught:
                    adapter.run(NodeSpec("n", "work"), {})
                self.assertEqual(caught.exception.kind, "sse")

    def test_event_order_fails_closed(self) -> None:
        _FakeHandler.payload = _sse(reverse_tail=True)
        adapter = self._adapter()
        result, _ = self._run(adapter)
        self.assertEqual(result.status, "failed")
        self.assertEqual(adapter.calls[-1]["kind"], "sse")

    def test_standard_sse_framing_and_split_usage(self) -> None:
        payload = _sse().replace(
            b'{"type":"message_start"}',
            b'{"type":"message_start","message":{"usage":{"input_tokens":10,"output_tokens":0}}}',
        ).replace(b'{"input_tokens": 10, "output_tokens": 75}', b'{"output_tokens":75}')
        payload = payload.replace(b'data: {"usage": {"output_tokens":75}}', b'data: {"usage":\ndata: {"output_tokens":75}}')
        for newline in (b'\n', b'\r\n', b'\r'):
            with self.subTest(newline=newline):
                _FakeHandler.payload = b'\xef\xbb\xbf' + payload.replace(b'\n', newline)
                result = self._adapter().run(NodeSpec("n", "work"), {})
                self.assertEqual(result.envelope["usage"]["total_tokens"], 85)

    def test_unterminated_sse_stop_is_not_a_complete_event(self) -> None:
        _FakeHandler.payload = _sse().rstrip(b'\n')
        with self.assertRaises(AdapterFailure) as caught:
            self._adapter().run(NodeSpec("n", "work"), {})
        self.assertEqual(caught.exception.kind, "sse")

    def test_missing_usage_fails_closed(self) -> None:
        _FakeHandler.payload = _sse(include_usage=False)
        adapter = self._adapter()
        result, _ = self._run(adapter)
        self.assertEqual(result.status, "failed")
        self.assertEqual(adapter.calls[-1]["kind"], "usage")

    def test_late_usage_cannot_decrease_or_discard_reported_counts(self) -> None:
        stop = b'event: message_stop\ndata: {"type":"message_stop"}\n\n'
        for usage in ({"output_tokens": 1}, [], None, {"input_tokens": False}):
            with self.subTest(usage=usage):
                update = ('event: message_delta\ndata: ' + json.dumps({"usage": usage}) + '\n\n').encode()
                _FakeHandler.payload = _sse().replace(stop, update + stop)
                with self.assertRaises(AdapterFailure) as caught:
                    self._adapter().run(NodeSpec("n", "work"), {})
                self.assertEqual(caught.exception.kind, "usage")

    def test_invalid_start_usage_cannot_be_replaced_by_valid_final_usage(self) -> None:
        _FakeHandler.payload = _sse().replace(
            b'{"type":"message_start"}',
            b'{"type":"message_start","message":{"usage":{"input_tokens":false,"output_tokens":0}}}',
        )
        with self.assertRaises(AdapterFailure) as caught:
            self._adapter().run(NodeSpec("n", "work"), {})
        self.assertEqual(caught.exception.kind, "usage")

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
        for extractor in (False, object()):
            with self.subTest(extractor=extractor), self.assertRaises(ValueError):
                self._adapter(answer_extractor=extractor)
        for name in ("timeout", "cost_per_1k_tokens"):
            for value in (float("nan"), float("inf"), float("-inf"), 10 ** 400):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    HttpWorkerAdapter(self.url, "model", **{name: value})
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
