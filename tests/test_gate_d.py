"""Offline tests for the Gate D capacity harness.

回环服务器扮演模型：可以全部成功，也可以按固定规则返回 429，从而在零外部调用的
前提下覆盖"返回率曲线"与"失败分类"两条路径。
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import benchmark_gate_d as gate_d


def _sse(answer: str, *, output_tokens: int = 9) -> bytes:
    frames = [
        'event: message_start\ndata: {"type":"message_start"}\n\n',
        'event: content_block_start\ndata: {"type":"content_block_start"}\n\n',
        f'event: content_block_delta\ndata: {json.dumps({"delta": {"type": "text_delta", "text": answer}})}\n\n',
        'event: content_block_stop\ndata: {"type":"content_block_stop"}\n\n',
        'event: message_delta\ndata: '
        + json.dumps({"usage": {"input_tokens": 20, "output_tokens": output_tokens}})
        + "\n\n",
        'event: message_stop\ndata: {"type":"message_stop"}\n\n',
    ]
    return "".join(frames).encode("utf-8")


class _ModelHandler(BaseHTTPRequestHandler):
    throttle_every = 0  # 0 = 全部成功；N>0 表示每第 N 个请求返回 429
    request_count = 0

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        cls = self.__class__
        length = int(self.headers.get("content-length", "0"))
        self.rfile.read(length)
        cls.request_count += 1
        if cls.throttle_every and cls.request_count % cls.throttle_every == 0:
            body = b'{"error":"rate limited"}'
            self.send_response(429)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        payload = _sse('{"answer":"Paris"}')
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args: object) -> None:
        return


def _args(directory: Path, **overrides: Any) -> argparse.Namespace:
    options: dict[str, Any] = {
        "endpoint": "http://127.0.0.1:1/v1/messages",
        "output": directory / "gate-d.json",
        "model": "fake-model",
        "levels": "4,8",
        "timeout": 5.0,
        "deadline": 120.0,
        "max_output_tokens": 200,
        "node_max_tokens": 20000,
        "budget_cost": 50.0,
        "cost_per_1k_tokens": 0.01,
        "workdir": directory / "state",
    }
    options.update(overrides)
    return argparse.Namespace(**options)


class GateDTests(unittest.TestCase):
    def setUp(self) -> None:
        _ModelHandler.throttle_every = 0
        _ModelHandler.request_count = 0
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _ModelHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/v1/messages"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_successful_levels_report_full_valid_rate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = gate_d.run(_args(Path(directory), endpoint=self.url))
        self.assertNotIn("error", report)
        self.assertEqual([entry["level"] for entry in report["levels"]], [4, 8])
        for entry in report["levels"]:
            self.assertEqual(entry["returned_valid"], entry["level"])
            self.assertEqual(entry["valid_rate"], 1.0)
            self.assertEqual(entry["failure_kinds"], {})
            self.assertTrue(entry["budget_invariant_ok"])

    def test_peak_concurrency_matches_the_level(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = gate_d.run(_args(Path(directory), endpoint=self.url, levels="8"))
        self.assertEqual(report["levels"][0]["peak_concurrency"], 8)

    def test_throttled_endpoint_reports_failure_kinds(self) -> None:
        _ModelHandler.throttle_every = 4
        with tempfile.TemporaryDirectory() as directory:
            report = gate_d.run(_args(Path(directory), endpoint=self.url, levels="8"))
        entry = report["levels"][0]
        self.assertLess(entry["valid_rate"], 1.0)
        self.assertEqual(entry["failure_kinds"].get("http"), 2)
        self.assertEqual(entry["failed_nodes"], 2)
        self.assertEqual(len(entry["adapter_calls"]), 8)

    def test_capacity_summary_carries_the_curve(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = gate_d.run(_args(Path(directory), endpoint=self.url, levels="4,8"))
        summary = report["capacity_summary"]
        self.assertEqual([item["level"] for item in summary], [4, 8])
        self.assertTrue(all(item["valid_rate"] == 1.0 for item in summary))

    def test_preflight_rejects_insufficient_budget_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError) as caught:
                gate_d.run(_args(Path(directory), endpoint=self.url, node_max_tokens=100))
        self.assertIn("--node-max-tokens", str(caught.exception))
        self.assertEqual(_ModelHandler.request_count, 0)

    def test_levels_parsing_is_fail_closed(self) -> None:
        self.assertEqual(gate_d.parse_levels("64, 8 ,16"), [8, 16, 64])
        for bad in ("", "0", "8,8", "-4"):
            with self.assertRaises(ValueError):
                gate_d.parse_levels(bad)

    def test_main_writes_report_and_prints_ascii(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.json"
            argv = sys.argv
            stream = io.StringIO()
            sys.argv = [
                "dsh-surge-gate-d",
                "--endpoint",
                self.url,
                "--output",
                str(output),
                "--levels",
                "4",
                "--node-max-tokens",
                "20000",
                "--max-output-tokens",
                "200",
            ]
            try:
                with contextlib.redirect_stdout(stream):
                    code = gate_d.main()
            finally:
                sys.argv = argv
            written = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(code, 0)
        self.assertTrue(stream.getvalue().isascii())
        self.assertEqual(written["levels"][0]["returned_valid"], 4)

    def test_limitations_are_declared(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = gate_d.run(_args(Path(directory), endpoint=self.url, levels="4"))
        self.assertTrue(any("not a quota" in item for item in report["limitations"]))
        self.assertEqual(report["adapter"]["max_attempts"], 1)


if __name__ == "__main__":
    unittest.main()
