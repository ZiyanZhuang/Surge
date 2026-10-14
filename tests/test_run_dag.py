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
import time
import unittest
from unittest import mock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import run_dag


class _FakeHandler(BaseHTTPRequestHandler):
    delay = 0.0
    payload = b""
    response_status = 200
    response_type = "text/event-stream"
    received: dict[str, Any] | None = None
    received_api_key: str | None = None

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        cls = self.__class__
        length = int(self.headers.get("content-length", "0"))
        cls.received = json.loads(self.rfile.read(length).decode("utf-8"))
        cls.received_api_key = self.headers.get("x-api-key")
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
    def test_main_cannot_overwrite_the_plan_or_its_hardlink(self) -> None:
        for hardlink in (False, True):
            with self.subTest(hardlink=hardlink):
                self._write_plan()
                before = self.plan.read_bytes()
                output = self.plan
                if hardlink:
                    output = self.directory / "plan-link.json"
                    output.hardlink_to(self.plan)
                stdout = io.StringIO()
                with mock.patch.object(sys, "argv", ["dsh-surge-run", "--plan", str(self.plan), "--output", str(output), "--dry-run"]), contextlib.redirect_stdout(stdout):
                    code = run_dag.main()
                self.assertEqual(self.plan.read_bytes(), before)
                self.assertEqual(code, 2)
                self.assertEqual(json.loads(stdout.getvalue())["error_kind"], "usage")

    def test_main_cannot_write_reports_over_runtime_state(self) -> None:
        self._write_plan()
        for name in ("run.sqlite3", "run.sqlite3-wal", "run-artifacts/report.json"):
            with self.subTest(name=name):
                workdir = self.directory / ("runtime-" + name.replace("/", "-"))
                output = workdir / name
                stdout = io.StringIO()
                with mock.patch.object(sys, "argv", ["dsh-surge-run", "--plan", str(self.plan), "--output", str(output), "--workdir", str(workdir), "--dry-run"]), contextlib.redirect_stdout(stdout):
                    code = run_dag.main()
                self.assertEqual(code, 2)
                self.assertEqual(json.loads(stdout.getvalue())["error_kind"], "usage")
                self.assertFalse(workdir.exists())

    def test_report_json_can_live_next_to_persisted_state(self) -> None:
        self._write_plan()
        workdir = self.directory / "runtime"
        output = workdir / "report.json"
        with mock.patch.object(sys, "argv", ["dsh-surge-run", "--plan", str(self.plan), "--output", str(output), "--workdir", str(workdir), "--dry-run"]), contextlib.redirect_stdout(io.StringIO()):
            code = run_dag.main()
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["result"]["status"], "succeeded")
        self.assertTrue((workdir / "run.sqlite3").is_file())

    def test_partial_report_write_keeps_the_previous_report(self) -> None:
        self._write_plan()
        previous = b'{"previous":"complete"}\n'
        self.output.write_bytes(previous)
        real_open = io.open

        def failing_open(file, mode="r", *args, **kwargs):
            handle = real_open(file, mode, *args, **kwargs)
            if mode != "w":
                return handle

            def partial_write(text):
                handle.write(text[:5])
                handle.flush()
                raise OSError("injected disk full")

            writer = mock.MagicMock(wraps=handle)
            writer.__enter__.return_value = writer
            writer.__exit__.side_effect = handle.__exit__
            writer.write.side_effect = partial_write
            return writer

        stdout = io.StringIO()
        with mock.patch.object(sys, "argv", ["dsh-surge-run", "--plan", str(self.plan), "--output", str(self.output), "--endpoint", self.url]), mock.patch("io.open", side_effect=failing_open), contextlib.redirect_stdout(stdout):
            code = run_dag.main()
        self.assertEqual(self.output.read_bytes(), previous)
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(stdout.getvalue())["error_kind"], "output")
        self.assertEqual(json.loads(stdout.getvalue())["result"]["status"], "succeeded")
        self.assertAlmostEqual(json.loads(stdout.getvalue())["result"]["spent_cost"], 0.0004)
        self.assertEqual(list(self.directory.glob(".report-*")), [])

    def test_report_replacement_preserves_other_hardlink_contents(self) -> None:
        self._write_plan()
        evidence = self.directory / "evidence.txt"
        evidence.write_text("immutable evidence", encoding="utf-8")
        self.output.hardlink_to(evidence)
        with mock.patch.object(sys, "argv", ["dsh-surge-run", "--plan", str(self.plan), "--output", str(self.output), "--dry-run"]), contextlib.redirect_stdout(io.StringIO()):
            code = run_dag.main()
        self.assertEqual(code, 0)
        self.assertEqual(evidence.read_text(encoding="utf-8"), "immutable evidence")
        self.assertEqual(json.loads(self.output.read_text(encoding="utf-8"))["result"]["status"], "succeeded")

    def setUp(self) -> None:
        _FakeHandler.delay = 0.0
        _FakeHandler.payload = _sse()
        _FakeHandler.response_status = 200
        _FakeHandler.response_type = "text/event-stream"
        _FakeHandler.received = None
        _FakeHandler.received_api_key = None
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

    def test_returned_report_does_not_share_live_adapter_call_list(self) -> None:
        class RecordingEcho(run_dag.LocalEchoAdapter):
            def __init__(self):
                self.calls = []

            def run(self, node, context):
                self.calls.append({"node_id": node.id})
                return super().run(node, context)

        self._write_plan()
        adapter = RecordingEcho()
        with mock.patch.object(run_dag, "_build_adapter", return_value=(adapter, None)):
            code, report = self._run("--dry-run")
        self.assertEqual(code, 0)
        count = len(report["adapter_calls"])
        adapter.calls.append({"node_id": "late"})
        self.assertEqual(len(report["adapter_calls"]), count)

    def test_cli_supports_documented_bridge_key_without_report_leak(self) -> None:
        self._write_plan()
        key = "test-bridge-auth-private-marker"
        code, report = self._run("--endpoint", self.url, "--api-key", key)
        self.assertEqual(code, 0)
        self.assertEqual(_FakeHandler.received_api_key, key)
        self.assertNotIn(key, json.dumps(report))
        self.assertEqual(report["adapter"]["api_key_placeholder"], "[redacted]")

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

    def test_invalid_numeric_options_have_the_same_usage_error_in_both_modes(self) -> None:
        self._write_plan()
        for mode in (("--dry-run",), ("--endpoint", self.url)):
            for option, values in (("--timeout", ("nan", "inf", "-inf", "0", "1e400")), ("--cost-per-1k-tokens", ("nan", "inf", "-inf", "-1", "1e400"))):
                for value in values:
                    with self.subTest(mode=mode, option=option, value=value):
                        code, report = self._run(*mode, f"{option}={value}")
                        self.assertEqual(code, 2)
                        self.assertEqual(report["error_kind"], "usage")
                        self.assertIn(option, report["error"])
        self.assertIsNone(_FakeHandler.received)

    def test_real_mode_requires_explicit_endpoint(self) -> None:
        self._write_plan()
        code, report = self._run()
        self.assertEqual(code, 2)
        self.assertEqual(report["error_kind"], "usage")
        self.assertIn("--endpoint", report["error"])

    def test_node_timeout_reaches_the_http_transport(self) -> None:
        mapping = _plan_mapping()
        for node in mapping["nodes"]:
            node.update(timeout_seconds=0.05, max_attempts=1)
        self._write_plan(mapping)
        _FakeHandler.delay = 0.2
        code, report = self._run("--endpoint", self.url)
        self.assertEqual(code, 1)
        self.assertEqual(report["adapter_calls"][0]["kind"], "transport")

    def test_rejected_completed_response_still_settles_cost(self) -> None:
        mapping = _plan_mapping()
        for node in mapping["nodes"]:
            node["max_attempts"] = 1
        self._write_plan(mapping)
        code, report = self._run("--endpoint", self.url, "--max-output-tokens", "4")
        self.assertEqual(code, 1)
        self.assertEqual(report["result"]["failed"], ["scout"])
        self.assertEqual(report["result"]["blocked"], ["synth"])
        self.assertAlmostEqual(report["result"]["spent_cost"], 20 / 1000 * 0.01)
        self.assertEqual(report["adapter_calls"][0]["kind"], "output-limit")

    def test_complete_stream_without_text_keeps_known_usage(self) -> None:
        _FakeHandler.payload = b"\n\n".join(part for part in _sse().split(b"\n\n") if not part.startswith(b"event: content_block_delta\n"))
        mapping = _plan_mapping()
        for node in mapping["nodes"]:
            node["max_attempts"] = 1
        self._write_plan(mapping)
        code, report = self._run("--endpoint", self.url)
        self.assertEqual(code, 1)
        self.assertEqual(report["adapter_calls"][0]["kind"], "sse")
        self.assertAlmostEqual(report["result"]["spent_cost"], 20 / 1000 * 0.01)

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

    def test_real_mode_accepts_a_plain_text_answer(self) -> None:
        _FakeHandler.payload = _sse("Evidence collected successfully.")
        mapping = _plan_mapping()
        mapping["nodes"][0]["max_attempts"] = 1
        self._write_plan(mapping)
        code, report = self._run("--endpoint", self.url)
        self.assertEqual(code, 0)
        self.assertEqual(report["result"]["succeeded"], ["scout", "synth"])

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

    def test_malformed_sse_cannot_succeed_through_the_cli(self) -> None:
        _FakeHandler.payload = _sse().replace(b'{"type":"message_start"}', b'not-json')
        mapping = _plan_mapping()
        mapping["nodes"][0]["max_attempts"] = 1
        self._write_plan(mapping)
        code, report = self._run("--endpoint", self.url)
        self.assertEqual(code, 1)
        self.assertEqual(report["result"]["failed"], ["scout"])
        self.assertEqual(report["adapter_calls"][0]["kind"], "sse")

    def test_cli_consumes_crlf_sse(self) -> None:
        _FakeHandler.payload = _sse().replace(b"\n", b"\r\n")
        self._write_plan()
        code, report = self._run("--endpoint", self.url)
        self.assertEqual(code, 0)
        self.assertEqual(report["result"]["status"], "succeeded")

    def test_cli_consumes_unicode_separator_inside_sse_text(self) -> None:
        _FakeHandler.payload = _sse("42\u2028units").replace(b"\\u2028", "\u2028".encode())
        self._write_plan()
        code, report = self._run("--endpoint", self.url)
        self.assertEqual(code, 0)
        self.assertEqual(report["result"]["status"], "succeeded")

    def test_cli_preserves_unicode_negative_answers(self) -> None:
        _FakeHandler.payload = _sse("\u221242")
        self._write_plan()
        code, report = self._run("--endpoint", self.url)
        self.assertEqual(code, 0)
        self.assertEqual(report["result"]["status"], "succeeded")
        self.assertEqual({claim["text"] for claim in report["provenance"]["claims"]}, {"\u221242"})

    def test_cli_rejects_decreasing_usage(self) -> None:
        stop = b'event: message_stop\ndata: {"type":"message_stop"}\n\n'
        update = b'event: message_delta\ndata: {"usage":{"output_tokens":0}}\n\n'
        _FakeHandler.payload = _sse().replace(stop, update + stop)
        mapping = _plan_mapping()
        mapping["nodes"][0]["max_attempts"] = 1
        self._write_plan(mapping)
        code, report = self._run("--endpoint", self.url)
        self.assertEqual(code, 1)
        self.assertEqual(report["adapter_calls"][0]["kind"], "usage")

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

    def test_malformed_plan_types_and_large_numbers_are_plan_errors(self) -> None:
        for field, value in (
            ("schema_version", True), ("validation_policy", []), ("validation_policy", {}),
            ("budget_cost", 10 ** 400), ("max_tokens", 10 ** 100),
        ):
            with self.subTest(field=field, value=value):
                mapping = _plan_mapping()
                if field == "schema_version":
                    mapping[field] = value
                elif field == "budget_cost":
                    mapping["run"][field] = value
                else:
                    mapping["nodes"][0][field] = value
                self._write_plan(mapping)
                code, report = self._run("--dry-run")
                self.assertEqual(code, 2)
                self.assertEqual(report["error_kind"], "plan")

    def test_unreadable_plan_content_returns_a_plan_error(self) -> None:
        for payload in (b"\xffinvalid UTF-8", b"[" * 2000 + b"]" * 2000, b'{"schema_version":' + b"1" * 5000 + b"}"):
            with self.subTest(size=len(payload)):
                self.plan.write_bytes(payload)
                code, report = self._run("--dry-run")
                self.assertEqual(code, 2)
                self.assertEqual(report["error_kind"], "plan")

    def test_plan_path_directory_returns_a_plan_error(self) -> None:
        self.plan.mkdir()
        code, report = self._run("--dry-run")
        self.assertEqual(code, 2)
        self.assertEqual(report["error_kind"], "plan")

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
        code, report = self._run("--dry-run")
        self.assertEqual(code, 2)
        self.assertEqual(report["error_kind"], "plan")
        self.assertIn("cycle", report["error"])

    def test_later_wave_dependency_returns_a_plan_error(self) -> None:
        mapping = _plan_mapping()
        mapping["nodes"][0]["wave"] = 2
        self._write_plan(mapping)
        code, report = self._run("--dry-run")
        self.assertEqual(code, 2)
        self.assertEqual(report["error_kind"], "plan")
        self.assertIn("later wave", report["error"])

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
