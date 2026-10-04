"""Tests for the ``dsh-surge-run`` CLI and the JSON task plan loader.

真实模式使用 127.0.0.1 上的临时回环服务器，不访问外部网络，也不需要凭据。
"""

from __future__ import annotations

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

import run_dag


class _FakeHandler(BaseHTTPRequestHandler):
    payload = b""
    response_status = 200
    response_type = "text/event-stream"
    received: dict[str, Any] | None = None

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        cls = self.__class__
        length = int(self.headers.get("content-length", "0"))
        cls.received = json.loads(self.rfile.read(length).decode("utf-8"))
        self.send_response(cls.response_status)
        self.send_header("content-type", cls.response_type)
        self.send_header("content-length", str(len(cls.payload)))
        self.end_headers()
        self.wfile.write(cls.payload)

    def log_message(self, *_args: object) -> None:
        return


def _sse(answer: str = "42") -> bytes:
    frames = [
        'event: message_start\ndata: {"type":"message_start"}\n\n',
        'event: content_block_start\ndata: {"type":"content_block_start"}\n\n',
        f'event: content_block_delta\ndata: {json.dumps({"delta": {"type": "text_delta", "text": answer}})}\n\n',
        'event: content_block_stop\ndata: {"type":"content_block_stop"}\n\n',
        'event: message_delta\ndata: {"usage": {"input_tokens": 12, "output_tokens": 8}}\n\n',
        'event: message_stop\ndata: {"type":"message_stop"}\n\n',
    ]
    return "".join(frames).encode("utf-8")


def _plan_mapping() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "description": "unit-test plan",
        "run": {"id": "cli-run", "budget_cost": 5.0, "max_workers": 2, "deadline_seconds": 60},
        "nodes": [
            {"id": "scout", "prompt": "collect evidence", "wave": 0, "max_tokens": 2000, "timeout_seconds": 5},
            {
                "id": "synth",
                "prompt": "summarize evidence",
                "wave": 1,
                "depends_on": ["scout"],
                "max_tokens": 2000,
                "timeout_seconds": 5,
            },
        ],
    }


class RunDagCliTests(unittest.TestCase):
    def setUp(self) -> None:
        _FakeHandler.payload = _sse()
        _FakeHandler.response_status = 200
        _FakeHandler.response_type = "text/event-stream"
        _FakeHandler.received = None
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/v1/messages"
        self.directory = Path(tempfile.mkdtemp(prefix="surge-run-dag-"))
        self.plan = self.directory / "plan.json"
        self.output = self.directory / "report.json"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def _write_plan(self, mapping: dict[str, Any] | None = None) -> Path:
        self.plan.write_text(json.dumps(mapping or _plan_mapping()), encoding="utf-8")
        return self.plan

    def _run(self, *extra: str) -> tuple[int, dict[str, Any]]:
        code, report, _ = run_dag.run(["--plan", str(self.plan), "--output", str(self.output), *extra])
        return code, report

    def test_dry_run_executes_plan_offline(self) -> None:
        self._write_plan()
        code, report = self._run("--dry-run")
        self.assertEqual(code, 0)
        self.assertEqual(report["mode"], "dry-run")
        self.assertIsNone(report["adapter"])
        self.assertEqual(report["result"]["status"], "succeeded")
        self.assertEqual(report["result"]["succeeded"], ["scout", "synth"])
        self.assertEqual(len(report["nodes"]), 2)
        self.assertEqual(len(report["plan"]["sha256"]), 64)
        self.assertTrue(report["generated_at_utc"].endswith("Z"))
        self.assertIn("python", report["environment"])

    def test_dry_run_does_not_contact_endpoint(self) -> None:
        self._write_plan()
        code, _ = self._run("--dry-run")
        self.assertEqual(code, 0)
        self.assertIsNone(_FakeHandler.received)

    def test_real_mode_requires_explicit_endpoint(self) -> None:
        self._write_plan()
        code, report = self._run()
        self.assertEqual(code, 2)
        self.assertEqual(report["error_kind"], "usage")
        self.assertIn("--endpoint", report["error"])

    def test_real_mode_runs_through_the_http_adapter(self) -> None:
        self._write_plan()
        code, report = self._run("--endpoint", self.url, "--model", "fake-model")
        self.assertEqual(code, 0)
        self.assertEqual(report["mode"], "http-adapter")
        self.assertEqual(report["result"]["status"], "succeeded")
        self.assertEqual(report["adapter"]["model"], "fake-model")
        self.assertEqual(report["adapter_calls"][0]["ok"], True)
        self.assertEqual(_FakeHandler.received["model"], "fake-model")
        self.assertEqual(_FakeHandler.received["max_tokens"], 2000)

    def test_real_mode_reports_failed_node_with_adapter_kind(self) -> None:
        _FakeHandler.payload = _sse()[:-40]
        self._write_plan()
        code, report = self._run("--endpoint", self.url)
        self.assertEqual(code, 1)
        self.assertEqual(report["result"]["status"], "failed")
        self.assertEqual(report["result"]["failed"], ["scout"])
        # 依赖未成功的下游节点被标记为 blocked，而不是 failed。
        self.assertEqual(report["result"]["blocked"], ["synth"])
        self.assertEqual(report["adapter_calls"][0]["kind"], "sse")

    def test_workdir_persists_runtime_state(self) -> None:
        self._write_plan()
        workdir = self.directory / "state"
        code, report = self._run("--dry-run", "--workdir", str(workdir))
        self.assertEqual(code, 0)
        self.assertTrue(report["runtime_state"]["persisted"])
        self.assertTrue((workdir / "run.sqlite3").is_file())

    def test_duplicate_node_id_is_rejected(self) -> None:
        mapping = _plan_mapping()
        mapping["nodes"][1]["id"] = "scout"
        self._write_plan(mapping)
        code, report = self._run("--dry-run")
        self.assertEqual(code, 2)
        self.assertEqual(report["error_kind"], "plan")
        self.assertIn("duplicated", report["error"])

    def test_unknown_dependency_is_rejected(self) -> None:
        mapping = _plan_mapping()
        mapping["nodes"][1]["depends_on"] = ["missing-node"]
        self._write_plan(mapping)
        code, report = self._run("--dry-run")
        self.assertEqual(code, 2)
        self.assertIn("unknown node", report["error"])

    def test_unknown_field_is_rejected(self) -> None:
        mapping = _plan_mapping()
        mapping["nodes"][0]["max_token"] = 2000
        self._write_plan(mapping)
        code, report = self._run("--dry-run")
        self.assertEqual(code, 2)
        self.assertIn("max_token", report["error"])

    def test_python_only_fields_get_an_explicit_hint(self) -> None:
        mapping = _plan_mapping()
        mapping["stage_policies"] = []
        self._write_plan(mapping)
        code, report = self._run("--dry-run")
        self.assertEqual(code, 2)
        self.assertIn("Python API", report["error"])

    def test_schema_version_is_enforced(self) -> None:
        mapping = _plan_mapping()
        mapping["schema_version"] = 2
        self._write_plan(mapping)
        code, report = self._run("--dry-run")
        self.assertEqual(code, 2)
        self.assertIn("schema_version", report["error"])

    def test_max_nodes_smaller_than_plan_is_rejected(self) -> None:
        mapping = _plan_mapping()
        mapping["run"]["max_nodes"] = 1
        self._write_plan(mapping)
        code, report = self._run("--dry-run")
        self.assertEqual(code, 2)
        self.assertIn("max_nodes", report["error"])

    def test_dependency_cycle_is_rejected_by_the_scheduler(self) -> None:
        mapping = _plan_mapping()
        mapping["nodes"] = [
            {"id": "a", "prompt": "a", "wave": 0, "depends_on": ["b"]},
            {"id": "b", "prompt": "b", "wave": 0, "depends_on": ["a"]},
        ]
        self._write_plan(mapping)
        with self.assertRaises(Exception):
            self._run("--dry-run")

    def test_main_writes_report_and_prints_ascii_json(self) -> None:
        self._write_plan()
        argv = sys.argv
        stream = io.StringIO()
        sys.argv = ["dsh-surge-run", "--plan", str(self.plan), "--output", str(self.output), "--dry-run"]
        try:
            with contextlib.redirect_stdout(stream):
                code = run_dag.main()
        finally:
            sys.argv = argv
        self.assertEqual(code, 0)
        printed = stream.getvalue()
        self.assertTrue(printed.isascii(), "stdout must stay ASCII so GBK consoles cannot mangle it")
        written = json.loads(self.output.read_text(encoding="utf-8"))
        self.assertEqual(written["tool"], "dsh-surge-run")
        self.assertEqual(written["result"]["status"], "succeeded")


if __name__ == "__main__":
    unittest.main()
