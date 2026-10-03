import json
import shutil
import sys
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmark_real_smoke import file_digest, load_manifest, run_case


class SmokeHarnessTests(unittest.TestCase):
    def _run_dir(self):
        directory = Path(__file__).resolve().parents[1] / f".smoke-test-run-{uuid.uuid4().hex}"
        directory.mkdir()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        return directory

    def record(self):
        return {
            "id": "unit-001",
            "pre_text": ["A company reported a value."],
            "post_text": ["The table contains the relevant figure."],
            "table": [["Metric", "Value"], ["Revenue", "100"]],
            "qa": {
                "question": "What is the value?",
                "answer": "100",
                "program": "add(40, 60)",
                "exe_ans": 100,
                "gold_inds": [["table", "Revenue"]],
            },
        }

    def test_replay_case_produces_a_passing_six_node_gate(self):
        report = run_case(self.record(), 1, self._run_dir())
        self.assertTrue(report["passed"])
        self.assertEqual(report["artifact_count"], 6)
        self.assertTrue(report["budget_invariant_ok"])
        self.assertEqual(report["result_status"], "succeeded")

    def test_manifest_binds_fixture_hash_and_record_order(self):
        directory = self._run_dir()
        fixture = directory / "smoke.json"
        fixture.write_text(json.dumps([self.record()], ensure_ascii=False) + "\n", encoding="utf-8")
        manifest = directory / "MANIFEST.json"
        payload = {
            "source_url": "https://github.com/czyssrs/FinQA",
            "revision": "test-revision",
            "split": "test",
            "fixture_sha256": file_digest(fixture),
            "record_ids": ["unit-001"],
        }
        manifest.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")
        loaded = load_manifest(manifest, fixture, [self.record()])
        self.assertEqual(loaded["fixture_sha256"], payload["fixture_sha256"])


if __name__ == "__main__":
    unittest.main()
