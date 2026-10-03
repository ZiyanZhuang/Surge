from __future__ import annotations

import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ReleaseHygieneTests(unittest.TestCase):
    def test_release_documents_and_fixture_notice_exist(self):
        required = [
            ROOT / "README.md",
            ROOT / "README.zh-CN.md",
            ROOT / "LICENSE",
            ROOT / "PHILOSOPHY.md",
            ROOT / "PHILOSOPHY.zh-CN.md",
            ROOT / "PHILOSOPHY-DIAGRAM-PROMPT.zh-CN.md",
            ROOT / "CONTRIBUTING.zh-CN.md",
            ROOT / "THIRD_PARTY_NOTICES.md",
            ROOT / "docs" / "QUICKSTART.zh-CN.md",
            ROOT / "docs" / "ADAPTER-CONTRACT.zh-CN.md",
            ROOT / "tests" / "fixtures" / "finqa" / "README.md",
        ]
        for path in required:
            self.assertTrue(path.is_file(), path)
        self.assertIn("FinQA", (ROOT / "THIRD_PARTY_NOTICES.md").read_text(encoding="utf-8"))

    def test_workflow_image_exists_and_is_linked_from_both_readmes(self):
        image_path = "docs/assets/surge-workflow.png"
        self.assertEqual((ROOT / image_path).read_bytes()[:8], b"\x89PNG\r\n\x1a\n")
        for name in ("README.md", "README.zh-CN.md"):
            with self.subTest(readme=name):
                self.assertIn(f"](<{image_path}>)", (ROOT / name).read_text(encoding="utf-8"))

    def test_fixture_manifest_has_provenance_and_license_status(self):
        manifest = json.loads((ROOT / "tests" / "fixtures" / "finqa" / "MANIFEST.json").read_text(encoding="utf-8"))
        for field in ("source_url", "revision", "split", "fixture_sha256", "record_ids", "license_status", "attribution"):
            self.assertIn(field, manifest)
        self.assertEqual(manifest["license_status"], "upstream-license-not-identified")
        self.assertTrue(manifest["record_ids"])

    def test_project_declares_mit_license_file_and_dev_build_extra(self):
        pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
        self.assertIn('license = "MIT"', pyproject)
        self.assertIn('license-files = ["LICENSE"]', pyproject)
        self.assertIn('"build==1.2.2"', pyproject)

    def test_no_unignored_runtime_or_secret_files_are_present(self):
        ignored_parts = {"__pycache__", ".venv", "build", "dist", ".release-build", "smoke-runs"}
        forbidden_suffixes = {".sqlite3", ".db", ".pem", ".key"}
        forbidden_names = {".env", "credentials.json", "secrets.json"}
        for path in ROOT.rglob("*"):
            if not path.is_file() or ignored_parts.intersection(path.parts):
                continue
            self.assertNotIn(path.name, forbidden_names, path)
            self.assertNotIn(path.suffix.lower(), forbidden_suffixes, path)

    def test_text_does_not_contain_user_specific_absolute_paths(self):
        user_path = re.compile(r"(?i)(?:[A-Z]:\\(?:Users|Documents and Settings)\\[^\r\n]+|/(?:home|Users)/[^/\s]+(?:/|$))")
        text_suffixes = {".md", ".py", ".json", ".jsonl", ".toml", ".yml", ".yaml", ".txt"}
        for path in ROOT.rglob("*"):
            if not path.is_file() or ".git" in path.parts or path.suffix.lower() not in text_suffixes:
                continue
            text = path.read_text(encoding="utf-8")
            self.assertIsNone(user_path.search(text), path)


if __name__ == "__main__":
    unittest.main()
