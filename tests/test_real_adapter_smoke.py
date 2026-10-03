from __future__ import annotations

import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import benchmark_real_adapter_smoke as smoke


class _FakeHandler(BaseHTTPRequestHandler):
    payload = b""
    response_status = 200
    response_type = "text/event-stream"
    received: dict[str, Any] | None = None

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        length = int(self.headers.get("content-length", "0"))
        self.__class__.received = json.loads(self.rfile.read(length).decode("utf-8"))
        self.send_response(self.__class__.response_status)
        self.send_header("content-type", self.__class__.response_type)
        self.send_header("content-length", str(len(self.__class__.payload)))
        self.end_headers()
        self.wfile.write(self.__class__.payload)

    def log_message(self, *_args: object) -> None:
        return


def _sse(*, answer: str = '{"answer":"93.5%"}', output_tokens: int = 75, complete: bool = True) -> bytes:
    frames = [
        'event: message_start\ndata: {"type":"message_start"}\n\n',
        'event: content_block_start\ndata: {"type":"content_block_start"}\n\n',
        f'event: content_block_delta\ndata: {json.dumps({"delta": {"type": "text_delta", "text": answer}})}\n\n',
        'event: content_block_stop\ndata: {"type":"content_block_stop"}\n\n',
        f'event: message_delta\ndata: {json.dumps({"usage": {"input_tokens": 10, "output_tokens": output_tokens}})}\n\n',
    ]
    if complete:
        frames.append('event: message_stop\ndata: {"type":"message_stop"}\n\n')
    return "".join(frames).encode("utf-8")


class RealAdapterSmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        _FakeHandler.payload = _sse()
        _FakeHandler.response_status = 200
        _FakeHandler.response_type = "text/event-stream"
        _FakeHandler.received = None
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/v1/messages"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_request_contains_explicit_output_bound(self):
        response = smoke.call_adapter(self.url, "test-model", "solve", 2)
        self.assertEqual(response["usage"]["output_tokens"], 75)
        self.assertEqual(_FakeHandler.received["max_tokens"], smoke.MAX_OUTPUT_TOKENS)
        self.assertIn("message_stop", response["events"])

    def test_missing_sse_event_fails_closed(self):
        _FakeHandler.payload = _sse(complete=False)
        with self.assertRaisesRegex(RuntimeError, "missing events"):
            smoke.call_adapter(self.url, "test-model", "solve", 2)

    def test_provider_output_over_limit_fails_closed(self):
        _FakeHandler.payload = _sse(output_tokens=smoke.MAX_OUTPUT_TOKENS + 1)
        with self.assertRaisesRegex(RuntimeError, "above requested limit"):
            smoke.call_adapter(self.url, "test-model", "solve", 2)

    def test_scheduler_persists_successful_adapter_result(self):
        with tempfile.TemporaryDirectory() as directory:
            result, adapter = smoke._execute_scheduler(
                prompt="solve", url=self.url, model="test-model", timeout=2, workdir=smoke.Path(directory)
            )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["succeeded"], ["model-answer"])
        self.assertIsNotNone(adapter.last_response)

    def test_scheduler_persists_malformed_adapter_failure(self):
        _FakeHandler.payload = _sse(complete=False)
        with tempfile.TemporaryDirectory() as directory:
            result, adapter = smoke._execute_scheduler(
                prompt="solve", url=self.url, model="test-model", timeout=2, workdir=smoke.Path(directory)
            )
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["failed"], ["model-answer"])
        self.assertIn("missing events", adapter.last_error or "")


if __name__ == "__main__":
    unittest.main()
