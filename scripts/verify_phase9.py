"""Phase 9 live verification: landing vs storage, transform naming, landing immutability."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import date
from pathlib import Path

from config.settings import get_settings
from hashing.files import sha256_bytes
from models.documents import transformed_key
from storage.mongo import MongoRepository
from storage.object_storage import S3ObjectStorage

REQUIRED_FIELDS = (
    "title",
    "description",
    "identifier",
    "date",
    "document_url",
    "partition_date",
    "body",
    "file_path",
    "file_hash",
)


def _records(repo: MongoRepository, start: date, end: date) -> list[dict]:
    return list(repo.iter_landing_by_partition_range(start, end))


def verify_landing(start: date, end: date) -> dict:
    settings = get_settings()
    repo = MongoRepository(settings)
    storage = S3ObjectStorage(settings)
    records = _records(repo, start, end)
    identities = [(r.get("body"), r.get("identifier")) for r in records]
    missing_objects = 0
    hash_mismatches = 0
    key_mismatches = 0
    metadata_gaps = 0
    snapshot = {}
    for record in records:
        if any(not record.get(field) for field in REQUIRED_FIELDS):
            metadata_gaps += 1
        key = record["file_path"]
        if not storage.exists(settings.s3_landing_bucket, key):
            missing_objects += 1
            continue
        payload = storage.get(settings.s3_landing_bucket, key)
        digest = sha256_bytes(payload)
        snapshot[key] = digest
        if digest != record["file_hash"]:
            hash_mismatches += 1
        if "/" not in key:
            key_mismatches += 1
    dupes = sum(1 for _, n in Counter(identities).items() if n > 1)
    report = {
        "landing_documents": len(records),
        "duplicate_identities": dupes,
        "missing_objects": missing_objects,
        "hash_mismatches": hash_mismatches,
        "landing_key_format_mismatches": key_mismatches,
        "metadata_gaps": metadata_gaps,
    }
    return report, snapshot


def verify_transformed(start: date, end: date, landing_snapshot: dict) -> dict:
    settings = get_settings()
    repo = MongoRepository(settings)
    storage = S3ObjectStorage(settings)
    landing = _records(repo, start, end)
    transformed = list(
        repo.transformed.find(
            {
                "partition_date": {
                    "$gte": start.isoformat(),
                    "$lt": end.isoformat(),
                }
            },
            projection={"_id": False},
        )
    )
    landing_changed = 0
    for record in landing:
        key = record["file_path"]
        payload = storage.get(settings.s3_landing_bucket, key)
        if sha256_bytes(payload) != landing_snapshot.get(key):
            landing_changed += 1
        if sha256_bytes(payload) != record["file_hash"]:
            landing_changed += 1

    missing_transformed = 0
    name_mismatches = 0
    hash_mismatches = 0
    for record in transformed:
        expected = transformed_key(record["identifier"], record.get("document_type") or "html")
        key = record["file_path"]
        if key != expected:
            name_mismatches += 1
        if not storage.exists(settings.s3_transformed_bucket, key):
            missing_transformed += 1
            continue
        payload = storage.get(settings.s3_transformed_bucket, key)
        if sha256_bytes(payload) != record["file_hash"]:
            hash_mismatches += 1

    return {
        "transformed_documents": len(transformed),
        "landing_documents": len(landing),
        "landing_bytes_changed_by_transform": landing_changed,
        "missing_transformed_objects": missing_transformed,
        "identifier_ext_mismatches": name_mismatches,
        "transformed_hash_mismatches": hash_mismatches,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--mode", choices=["landing", "transformed"], required=True)
    parser.add_argument("--snapshot", type=Path, default=Path("logs/p9-landing-snapshot.json"))
    args = parser.parse_args()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if args.mode == "landing":
        report, snapshot = verify_landing(start, end)
        args.snapshot.parent.mkdir(parents=True, exist_ok=True)
        args.snapshot.write_text(json.dumps(snapshot), encoding="utf-8")
        print(json.dumps(report, indent=2))
        bad = any(report[k] for k in report if k != "landing_documents")
        return 1 if bad else 0
    snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
    report = verify_transformed(start, end, snapshot)
    print(json.dumps(report, indent=2))
    bad = (
        report["landing_bytes_changed_by_transform"]
        or report["missing_transformed_objects"]
        or report["identifier_ext_mismatches"]
        or report["transformed_hash_mismatches"]
        or report["transformed_documents"] != report["landing_documents"]
    )
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
