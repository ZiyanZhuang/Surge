"""Select a reproducible, provenance-bound FinQA subset from a local archive.

This utility never downloads data and never invents benchmark records. It only
selects records that are present in the user-provided FinQA JSON/JSONL file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

OFFICIAL_SOURCE = "https://github.com/czyssrs/FinQA"
REQUIRED_QA_FIELDS = ("question", "answer", "program", "exe_ans")


def _normalise_id(value: Any, *, description: str) -> str:
    """Validate an ID and return the string form used by selection/manifests."""
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"{description} must be a non-boolean string or integer")
    if isinstance(value, str) and not value.strip():
        raise ValueError(f"{description} must not be blank")
    return str(value)


def _validate_record_ids(records: Sequence[Mapping[str, Any]]) -> list[str]:
    """Validate record IDs without silently collapsing duplicate records."""
    normalised: list[str] = []
    first_positions: dict[str, int] = {}
    for position, record in enumerate(records, start=1):
        if not isinstance(record, Mapping):
            raise ValueError(f"record {position} must be an object")
        record_id = _normalise_id(record.get("id"), description=f"record {position} id")
        if record_id in first_positions:
            raise ValueError(
                f"duplicate record id {record_id!r} at records {first_positions[record_id]} and {position}"
            )
        first_positions[record_id] = position
        normalised.append(record_id)
    return normalised


def load_records(path: Path) -> list[dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"FinQA archive not found: {path}")
    if path.suffix.lower() == ".jsonl":
        payload: Any = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict) and isinstance(payload.get("data"), list):
        records = payload["data"]
    else:
        raise ValueError("FinQA archive must be a JSON list, JSONL file, or object with a data list")
    if not records:
        raise ValueError("FinQA archive is empty")

    for position, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise ValueError(f"record {position} must be an object")
    record_ids = _validate_record_ids(records)
    for position, record in enumerate(records, start=1):
        record_id = record_ids[position - 1]
        if not isinstance(record.get("table"), list):
            raise ValueError(f"record {record_id} is missing table")
        qa = record.get("qa")
        if not isinstance(qa, Mapping) or any(field not in qa for field in REQUIRED_QA_FIELDS):
            raise ValueError(f"record {record_id} is missing one of qa.{', qa.'.join(REQUIRED_QA_FIELDS)}")
    return records


def select_records(records: list[dict[str, Any]], ids: list[str], limit: int) -> list[dict[str, Any]]:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("limit must be a positive integer")

    record_ids = _validate_record_ids(records)
    requested_ids: list[str] = []
    requested_positions: set[str] = set()
    for position, requested in enumerate(ids, start=1):
        requested_id = _normalise_id(requested, description=f"requested id {position}")
        if requested_id in requested_positions:
            raise ValueError(f"requested record id repeated: {requested_id}")
        requested_positions.add(requested_id)
        requested_ids.append(requested_id)

    if requested_ids:
        by_id = dict(zip(record_ids, records))
        missing = [record_id for record_id in requested_ids if record_id not in by_id]
        if missing:
            raise ValueError(f"requested record ids not found: {', '.join(missing)}")
        selected = [by_id[record_id] for record_id in requested_ids]
    else:
        selected = records[:limit]
    if not selected:
        raise ValueError("selection is empty")
    return selected


def _canonical_path(path: Path) -> str:
    """Return a case-normalised path suitable for alias comparisons."""
    return os.path.normcase(str(Path(path).expanduser().resolve(strict=False)))


def _path_exists(path: Path) -> bool:
    # lexists also treats a dangling symlink as an existing destination.
    return os.path.lexists(os.fspath(path))


def _remove_created(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        # Preserve the original write error; never replace it with cleanup noise.
        pass


def write_fixture(
    records: list[dict[str, Any]],
    output: Path,
    manifest: Path,
    *,
    source_url: str,
    revision: str,
    split: str,
    source_path: Path | None = None,
    license: str | None = None,
) -> dict[str, Any]:
    """Write a fixture and manifest without overwriting any existing path.

    ``source_path`` is optional for backwards compatibility. When supplied,
    the manifest records the local source file's SHA-256 and original filename.
    """
    output = Path(output)
    manifest = Path(manifest)
    source = Path(source_path) if source_path is not None else None
    if _canonical_path(output) == _canonical_path(manifest):
        raise ValueError("output and manifest must be different paths")
    if source is not None and _canonical_path(source) == _canonical_path(output):
        raise ValueError("input/source and output must be different paths")
    if source is not None and _canonical_path(source) == _canonical_path(manifest):
        raise ValueError("input/source and manifest must be different paths")
    for destination, label in ((output, "output"), (manifest, "manifest")):
        if _path_exists(destination):
            raise FileExistsError(f"refusing to overwrite existing {label}: {destination}")

    record_ids = _validate_record_ids(records)
    fixture_bytes = "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in records
    ).encode("utf-8")
    fixture_hash = hashlib.sha256(fixture_bytes).hexdigest()
    provenance: dict[str, Any] = {
        "source_url": source_url,
        "revision": revision,
        "split": split,
        "fixture_sha256": fixture_hash,
        "record_ids": record_ids,
    }
    if source is not None:
        source_bytes = source.read_bytes()
        provenance["source_sha256"] = hashlib.sha256(source_bytes).hexdigest()
        provenance["source_filename"] = source.name
    if license is not None:
        provenance["license"] = license
    manifest_bytes = (json.dumps(provenance, ensure_ascii=False, indent=2) + "\n").encode("utf-8")

    output.parent.mkdir(parents=True, exist_ok=True)
    manifest.parent.mkdir(parents=True, exist_ok=True)
    output_created = False
    manifest_created = False
    try:
        # Open both destinations exclusively before writing either one. If the
        # second open fails, the first newly-created file is removed again.
        with output.open("xb") as output_handle:
            output_created = True
            with manifest.open("xb") as manifest_handle:
                manifest_created = True
                output_handle.write(fixture_bytes)
                manifest_handle.write(manifest_bytes)
    except Exception:
        if manifest_created:
            _remove_created(manifest)
        if output_created:
            _remove_created(output)
        raise
    return provenance


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="local FinQA JSON/JSONL archive")
    parser.add_argument("--output", type=Path, required=True, help="selected JSONL fixture")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--revision", required=True, help="upstream commit, tag, or archive version")
    parser.add_argument("--split", required=True, help="upstream split, e.g. train/dev/test")
    parser.add_argument("--source-url", default=OFFICIAL_SOURCE)
    parser.add_argument("--license", dest="license_name", help="license metadata stated by the data provider")
    parser.add_argument("--id", dest="ids", action="append", default=[], help="select an exact record id; repeatable")
    parser.add_argument("--limit", type=int, default=3)
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("--limit must be positive")
    try:
        selected = select_records(load_records(args.input), args.ids, args.limit)
        provenance = write_fixture(
            selected,
            args.output,
            args.manifest,
            source_url=args.source_url,
            revision=args.revision,
            split=args.split,
            source_path=args.input,
            license=args.license_name,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    # stdout 使用 ASCII 转义，避免在 cp936/GBK 控制台或管道中把中文路径写成乱码字节。
    print(json.dumps({"selected": len(selected), "output": str(args.output), "manifest": provenance}, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
