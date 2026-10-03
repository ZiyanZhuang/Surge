"""Verify that source distribution and wheel contain release-critical files."""

from __future__ import annotations

import argparse
import json
import tarfile
import zipfile
from pathlib import Path

REQUIRED_SUFFIXES = (
    "README.md",
    "README.zh-CN.md",
    "LICENSE",
    "PHILOSOPHY.md",
    "PHILOSOPHY.zh-CN.md",
    "CONTRIBUTING.zh-CN.md",
    "THIRD_PARTY_NOTICES.md",
    "docs/QUICKSTART.zh-CN.md",
    "docs/ADAPTER-CONTRACT.zh-CN.md",
    "tests/fixtures/finqa/smoke.jsonl",
    "tests/fixtures/finqa/MANIFEST.json",
    "tests/fixtures/finqa/README.md",
)


def _check_names(label: str, names: list[str]) -> list[str]:
    missing = [suffix for suffix in REQUIRED_SUFFIXES if not any(name.endswith(suffix) for name in names)]
    if missing:
        raise SystemExit(f"{label}: missing required files: {', '.join(missing)}")
    return names


def _verify_manifest_from_archive(label: str, data: bytes) -> None:
    manifest = json.loads(data.decode("utf-8"))
    required = {"source_url", "revision", "split", "fixture_sha256", "record_ids", "license_status", "attribution"}
    missing = sorted(required - set(manifest))
    if missing:
        raise SystemExit(f"{label}: fixture manifest missing fields: {', '.join(missing)}")
    if manifest["license_status"] != "upstream-license-not-identified":
        raise SystemExit(f"{label}: unexpected fixture license status")


def verify_archive(path: Path) -> None:
    if path.suffix == ".gz" and path.name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as archive:
            members = archive.getmembers()
            _check_names(str(path), [member.name for member in members])
            manifest_member = next(member for member in members if member.name.endswith("tests/fixtures/finqa/MANIFEST.json"))
            extracted = archive.extractfile(manifest_member)
            if extracted is None:
                raise SystemExit(f"{path}: cannot read fixture manifest")
            _verify_manifest_from_archive(str(path), extracted.read())
        return
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            _check_names(str(path), archive.namelist())
            manifest_name = next(name for name in archive.namelist() if name.endswith("tests/fixtures/finqa/MANIFEST.json"))
            _verify_manifest_from_archive(str(path), archive.read(manifest_name))
        return
    raise SystemExit(f"unsupported release file: {path}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist", type=Path, required=True)
    args = parser.parse_args()
    archives = sorted(path for path in args.dist.iterdir() if path.name.endswith((".tar.gz", ".whl")))
    if not archives:
        raise SystemExit(f"no sdist or wheel found in {args.dist}")
    for archive in archives:
        verify_archive(archive)
    print(f"release verification passed: {len(archives)} archives")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
