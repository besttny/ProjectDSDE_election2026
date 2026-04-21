from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from src.pipeline.config import ProjectConfig


@dataclass(frozen=True)
class ManifestEntry:
    form_type: str
    vote_type: str
    required: bool
    expected_polling_stations: int | None
    file_path: Path
    source_url: str
    notes: str = ""

    @property
    def exists(self) -> bool:
        return self.file_path.exists()


def _as_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _as_optional_int(value: str) -> int | None:
    value = str(value).strip()
    return int(value) if value else None


def load_manifest(config: ProjectConfig) -> list[ManifestEntry]:
    manifest_path = config.path("manifest")
    with manifest_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        entries: list[ManifestEntry] = []
        for row in reader:
            entries.append(
                ManifestEntry(
                    form_type=row["form_type"].strip(),
                    vote_type=row["vote_type"].strip(),
                    required=_as_bool(row.get("required", "")),
                    expected_polling_stations=_as_optional_int(
                        row.get("expected_polling_stations", "")
                    ),
                    file_path=config.resolve(row["file_path"].strip()),
                    source_url=row.get("source_url", "").strip(),
                    notes=row.get("notes", "").strip(),
                )
            )
    return entries


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def manifest_status_rows(entries: Iterable[ManifestEntry]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for entry in entries:
        rows.append(
            {
                "form_type": entry.form_type,
                "vote_type": entry.vote_type,
                "required": entry.required,
                "expected_polling_stations": entry.expected_polling_stations or "",
                "file_path": str(entry.file_path),
                "exists": entry.exists,
                "sha256": sha256_file(entry.file_path) if entry.exists else "",
                "source_url": entry.source_url,
                "notes": entry.notes,
            }
        )
    return rows


def write_manifest_status(config: ProjectConfig, entries: Iterable[ManifestEntry]) -> Path:
    config.ensure_output_dirs()
    output_path = config.output("validation_report").parent / "manifest_status.csv"
    rows = manifest_status_rows(entries)
    fieldnames = [
        "form_type",
        "vote_type",
        "required",
        "expected_polling_stations",
        "file_path",
        "exists",
        "sha256",
        "source_url",
        "notes",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def missing_required_entries(entries: Iterable[ManifestEntry]) -> list[ManifestEntry]:
    return [entry for entry in entries if entry.required and not entry.exists]

