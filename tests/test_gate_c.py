"""Offline tests for the Gate C harness.

回环服务器扮演"理想模型"：它从收到的提示里取出 FinQA ``program``，用项目自带的
受限 oracle 算出答案再返回。这样可以在不接触任何外部 provider 的前提下，把
Gate C 的调度、并发上界、预算不变量、独立复核和 provenance 全部跑通。
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

import benchmark_gate_c as gate_c
from surge_cluster.finqa import execute_program

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "finqa" / "smoke.jsonl"
MANIFEST = Path(__file__).resolve().parent / "fixtures" / "finqa" / "MANIFEST.json"


def _sse(answer: str, *, output_tokens: int = 40) -> bytes:
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


class _IdealModelHandler(BaseHTTPRequestHandler):
    mode = "ideal"
    request_count = 0

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        cls = self.__class__
        length = int(self.headers.get("content-length", "0"))
        body = json.loads(self.rfile.read(length).decode("utf-8"))
        cls.request_count += 1
        text = body["messages"][0]["content"][0]["text"]
        program = ""
        for line in text.splitlines():
            if line.startswith("program: "):
                program = line[len("program: ") :]
        answer = json.dumps({"answer": execute_program(program)["value"]})
        if cls.mode == "wrong":
            answer = json.dumps({"answer": "999999"})
        payload = _sse(answer)
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args: object) -> None:
        return


def _args(directory: Path, **overrides: Any) -> argparse.Namespace:
    options: dict[str, Any] = {
        "fixture": FIXTURE,
        "manifest": MANIFEST,
        "output": directory / "gate-c.json",
        "endpoint": "http://127.0.0.1:1/v1/messages",
        "model": "fake-model",
        "cases": 3,
        "max_workers": 2,
        "timeout": 5.0,
        "deadline": 60.0,
        "max_output_tokens": 200,
        "node_max_tokens": 20000,
        "budget_cost": 5.0,
        "cost_per_1k_tokens": 0.01,
        "isolate": False,
        "workdir": directory / "state",
    }
    options.update(overrides)
    return argparse.Namespace(**options)


class GateCTests(unittest.TestCase):
    def setUp(self) -> None:
        _IdealModelHandler.mode = "ideal"
        _IdealModelHandler.request_count = 0
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _IdealModelHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/v1/messages"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_ideal_model_passes_all_three_cases(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = gate_c.run(_args(Path(directory), endpoint=self.url))
        self.assertTrue(report["passed"], report.get("error"))
        self.assertEqual(report["cases_passed"], 3)
        self.assertEqual(len(report["cases"]), 3)
        self.assertEqual(report["result"]["status"], "succeeded")
        self.assertEqual(_IdealModelHandler.request_count, 3)

    def test_bounded_concurrency_is_respected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = gate_c.run(_args(Path(directory), endpoint=self.url, max_workers=1))
        self.assertTrue(report["passed"])
        self.assertEqual(report["concurrency"]["configured_max_workers"], 1)
        self.assertEqual(report["concurrency"]["observed_peak_adapter_calls"], 1)

    def test_budget_invariant_and_heartbeats_are_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = gate_c.run(_args(Path(directory), endpoint=self.url))
        self.assertTrue(report["budget_invariant_ok"])
        self.assertTrue(report["budget"]["invariant_ok"])
        self.assertLessEqual(report["budget"]["spent_cost"], report["budget"]["budget_cost"])
        self.assertGreaterEqual(report["budget"]["ledger_entries"], 1)
        self.assertIn("task.heartbeat", report["event_counts"])
        self.assertEqual(report["heartbeat_events"], report["event_counts"]["task.heartbeat"])

    def test_independent_check_reports_usage_and_verification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = gate_c.run(_args(Path(directory), endpoint=self.url))
        first = report["cases"][0]
        self.assertEqual(first["usage"]["output_tokens"], 40)
        self.assertTrue(first["oracle_ok"])
        self.assertTrue(first["answer_check"]["ok"])
        self.assertEqual(report["verification"]["nodes_verified"], 6)
        self.assertIn("self_reference", json.dumps(report["verification"]))

    def test_provenance_covers_claims_and_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = gate_c.run(_args(Path(directory), endpoint=self.url))
        provenance = report["provenance"]
        self.assertTrue(provenance["claims_available"])
        self.assertEqual(provenance["artifact_count"], 6)
        self.assertEqual(provenance["claim_count"], 3)
        # check 节点自己的 artifact 没有被任何 claim 引用，属于信息性发现。
        codes = {finding["code"] for finding in provenance["findings"]}
        self.assertEqual(codes, {"uncited_artifact"})
        self.assertEqual(provenance["finding_count"], 3)

    def test_wrong_model_answer_fails_the_case_and_the_run(self) -> None:
        _IdealModelHandler.mode = "wrong"
        with tempfile.TemporaryDirectory() as directory:
            report = gate_c.run(_args(Path(directory), endpoint=self.url))
        self.assertFalse(report["passed"])
        self.assertEqual(report["cases_passed"], 0)
        self.assertEqual(report["result"]["status"], "failed")
        self.assertEqual(sorted(report["result"]["failed"]), ["case1/check", "case2/check", "case3/check"])

    def test_preflight_rejects_insufficient_budget_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError) as caught:
                gate_c.run(_args(Path(directory), endpoint=self.url, node_max_tokens=100))
        self.assertIn("--node-max-tokens", str(caught.exception))
        self.assertEqual(_IdealModelHandler.request_count, 0)

    def test_isolated_mode_keeps_call_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = gate_c.run(
                _args(Path(directory), endpoint=self.url, cases=1, max_workers=1, isolate=True)
            )
        self.assertTrue(report["passed"], report.get("error"))
        self.assertTrue(report["adapter"]["isolated"])
        self.assertEqual(len(report["adapter_calls"]), 1)
        self.assertTrue(report["adapter_calls"][0]["ok"])

    def test_main_writes_report_and_prints_ascii(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.json"
            argv = sys.argv
            stream = io.StringIO()
            sys.argv = [
                "dsh-surge-gate-c",
                "--fixture",
                str(FIXTURE),
                "--manifest",
                str(MANIFEST),
                "--output",
                str(output),
                "--endpoint",
                self.url,
                "--cases",
                "1",
                "--max-workers",
                "1",
                "--node-max-tokens",
                "20000",
                "--max-output-tokens",
                "200",
            ]
            try:
                with contextlib.redirect_stdout(stream):
                    code = gate_c.main()
            finally:
                sys.argv = argv
            written = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(code, 0)
        self.assertTrue(stream.getvalue().isascii())
        self.assertTrue(written["passed"])


if __name__ == "__main__":
    unittest.main()
