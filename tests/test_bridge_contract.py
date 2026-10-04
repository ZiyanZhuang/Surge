"""Cross-language contract test for the DSH ``llm.stream`` bridge.

Node 侧的 ``dsh-bridge/selftest.mjs`` 负责证明 bridge 的真实输出；这里回放它写出的
``golden-sse.txt``，证明 Python 侧已发布的 ``HttpWorkerAdapter`` 能原样消费该格式。
两端因此被钉在同一个线格式上：任何一侧擅自改变格式，都会有一侧的测试失败。
"""

from __future__ import annotations

import json
import re
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from surge_cluster import DAGScheduler, HttpWorkerAdapter, NodeSpec, RunSpec

ROOT = Path(__file__).resolve().parents[1]
BRIDGE_DIR = ROOT / "dsh-bridge"
GOLDEN = BRIDGE_DIR / "golden-sse.txt"
PLUGIN = BRIDGE_DIR / "dsh-llm-bridge.mjs"
REQUIRED_EVENTS = ("message_start", "content_block_delta", "message_delta", "message_stop")


class _GoldenHandler(BaseHTTPRequestHandler):
    payload = b""

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        length = int(self.headers.get("content-length", "0"))
        self.rfile.read(length)
        self.send_response(200)
        self.send_header("content-type", "text/event-stream; charset=utf-8")
        self.send_header("content-length", str(len(self.__class__.payload)))
        self.end_headers()
        self.wfile.write(self.__class__.payload)

    def log_message(self, *_args: object) -> None:
        return


class BridgeContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.golden = GOLDEN.read_text(encoding="utf-8")
        _GoldenHandler.payload = self.golden.encode("utf-8")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _GoldenHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/v1/messages"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_golden_has_the_required_event_order(self) -> None:
        events = re.findall(r"^event: (.+)$", self.golden, re.MULTILINE)
        positions = [events.index(name) for name in REQUIRED_EVENTS]
        self.assertEqual(positions, sorted(positions), events)
        self.assertTrue(all(index >= 0 for index in positions))

    def test_golden_uses_snake_case_usage(self) -> None:
        message_delta = next(
            json.loads(line[6:])
            for line in self.golden.splitlines()
            if line.startswith("data: ") and '"message_delta"' in line
        )
        usage = message_delta["usage"]
        self.assertEqual(set(usage), {"input_tokens", "output_tokens"})
        self.assertIsInstance(usage["input_tokens"], int)
        self.assertIsInstance(usage["output_tokens"], int)

    def test_published_adapter_consumes_bridge_output(self) -> None:
        # bridge 的 stub 答案是自由文本 "Hello"，因此使用默认提取器（非纯数字提取器）。
        adapter = HttpWorkerAdapter(
            self.url,
            "stub-model",
            timeout=5.0,
            warning="bridge contract test",
        )
        with tempfile.TemporaryDirectory() as directory:
            run = RunSpec(id="bridge", budget_cost=5.0, max_nodes=1, max_workers=1, deadline_seconds=30.0)
            node = NodeSpec(id="n1", prompt="say hello", max_attempts=1, timeout_seconds=10.0, max_tokens=4000)
            with DAGScheduler(Path(directory) / "run.sqlite3", max_workers=1) as scheduler:
                scheduler.submit(run, [node])
                result = scheduler.execute(adapter)
                stored = scheduler.envelopes("bridge")
        self.assertEqual(result.status, "succeeded", result.events)
        self.assertEqual(stored["n1"]["envelope"]["answer"], "Hello")
        self.assertAlmostEqual(result.spent_cost, (12 + 7) / 1000.0 * adapter.cost_per_1k_tokens, places=9)
        self.assertEqual(adapter.calls[0]["usage"], {"input_tokens": 12, "output_tokens": 7})

    def test_plugin_binds_the_documented_service(self) -> None:
        source = PLUGIN.read_text(encoding="utf-8")
        self.assertIn("export const inject = ['llm']", source)
        self.assertIn("ctx.llm.stream(", source)
        self.assertIn("127.0.0.1", source)

    def test_plugin_never_writes_prompt_or_token_to_logs(self) -> None:
        source = PLUGIN.read_text(encoding="utf-8")
        self.assertNotIn("console.log", source)
        self.assertNotIn("console.error", source)


if __name__ == "__main__":
    unittest.main()
