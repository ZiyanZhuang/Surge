import hashlib
import json
import shutil
import sys
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmark_real_smoke import load_manifest
from select_finqa_fixture import load_records, main, select_records, write_fixture


class FinQASelectorTests(unittest.TestCase):
    def _run_dir(self):
        directory = Path(__file__).resolve().parents[1] / f".finqa-selector-test-{uuid.uuid4().hex}"
        directory.mkdir()
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        return directory

    def record(self, record_id):
        return {
            "id": record_id,
            "pre_text": [],
            "post_text": [],
            "table": [["Metric", "Value"], ["Revenue", "100"]],
            "qa": {
                "question": "What is the value?",
                "answer": "100",
                "program": "add(40, 60)",
                "exe_ans": 100,
            },
        }

    def test_select_by_exact_ids_and_write_provenance(self):
        directory = self._run_dir()
        records = [self.record("a"), self.record("b")]
        selected = select_records(records, ["b"], 3)
        fixture = directory / "smoke.jsonl"
        manifest = directory / "MANIFEST.json"
        provenance = write_fixture(
            selected,
            fixture,
            manifest,
            source_url="https://github.com/czyssrs/FinQA",
            revision="test-revision",
            split="dev",
        )
        self.assertEqual(provenance["record_ids"], ["b"])
        self.assertEqual(load_records(fixture)[0]["id"], "b")
        self.assertEqual(json.loads(manifest.read_text(encoding="utf-8"))["fixture_sha256"], provenance["fixture_sha256"])

    def test_unknown_id_is_rejected(self):
        with self.assertRaises(ValueError):
            select_records([self.record("a")], ["missing"], 3)

    def test_invalid_and_duplicate_archive_ids_are_rejected(self):
        directory = self._run_dir()
        for records in (
            [self.record(" ")],
            [self.record(True)],
            [self.record("same"), self.record("same")],
            [self.record(7), self.record("7")],
        ):
            archive = directory / f"archive-{uuid.uuid4().hex}.json"
            archive.write_text(json.dumps(records), encoding="utf-8")
            with self.subTest(records=records), self.assertRaises(ValueError):
                load_records(archive)

    def test_repeated_requested_id_is_rejected(self):
        with self.assertRaises(ValueError):
            select_records([self.record("a"), self.record("b")], ["a", "a"], 3)

    def test_api_limit_must_be_positive_integer(self):
        records = [self.record("a")]
        for limit in (0, -1, True, 1.5):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                select_records(records, [], limit)

    def test_output_and_manifest_are_no_overwrite_and_not_aliases(self):
        directory = self._run_dir()
        output = directory / "smoke.jsonl"
        manifest = directory / "MANIFEST.json"
        output.write_text("keep", encoding="utf-8")
        with self.assertRaises(FileExistsError):
            write_fixture(
                [self.record("a")],
                output,
                manifest,
                source_url="https://github.com/czyssrs/FinQA",
                revision="revision",
                split="dev",
            )
        self.assertEqual(output.read_text(encoding="utf-8"), "keep")
        self.assertFalse(manifest.exists())

        alias_output = directory / "nested" / "smoke.jsonl"
        alias_manifest = directory / "nested" / "." / "smoke.jsonl"
        with self.assertRaises(ValueError):
            write_fixture(
                [self.record("a")],
                alias_output,
                alias_manifest,
                source_url="https://github.com/czyssrs/FinQA",
                revision="revision",
                split="dev",
            )
        self.assertFalse(alias_output.exists())

    def test_cli_rejects_input_output_alias(self):
        directory = self._run_dir()
        source = directory / "archive.json"
        source.write_text(json.dumps([self.record("a")]), encoding="utf-8")
        alias = source.parent / "." / source.name
        with self.assertRaises(SystemExit):
            main(
                [
                    "--input",
                    str(source),
                    "--output",
                    str(alias),
                    "--manifest",
                    str(directory / "MANIFEST.json"),
                    "--revision",
                    "revision",
                    "--split",
                    "dev",
                ]
            )
        self.assertEqual(json.loads(source.read_text(encoding="utf-8"))[0]["id"], "a")
        self.assertFalse((directory / "MANIFEST.json").exists())

    def test_cli_manifest_has_source_provenance_and_is_consumable(self):
        directory = self._run_dir()
        source = directory / "original-dev.json"
        source_bytes = json.dumps([self.record("a"), self.record("b")], ensure_ascii=False).encode("utf-8")
        source.write_bytes(source_bytes)
        fixture = directory / "smoke.jsonl"
        manifest = directory / "MANIFEST.json"
        main(
            [
                "--input",
                str(source),
                "--output",
                str(fixture),
                "--manifest",
                str(manifest),
                "--revision",
                "revision",
                "--split",
                "dev",
                "--id",
                "b",
            ]
        )
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        self.assertEqual(payload["source_sha256"], hashlib.sha256(source_bytes).hexdigest())
        self.assertEqual(payload["source_filename"], source.name)
        self.assertNotIn("license", payload)
        consumed = load_manifest(manifest, fixture, load_records(fixture))
        self.assertEqual(consumed["record_ids"], ["b"])


if __name__ == "__main__":
    unittest.main()
