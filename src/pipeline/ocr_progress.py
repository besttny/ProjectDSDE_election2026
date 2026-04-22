from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.pipeline.config import ProjectConfig, load_config
from src.pipeline.manifest import ManifestEntry, load_manifest

try:
    import fitz
except ImportError:  # pragma: no cover - PyMuPDF is installed in the project env
    fitz = None

try:
    from pypdf import PdfReader
except ImportError:  # pragma: no cover - optional local reporting fallback
    PdfReader = None


PROGRESS_COLUMNS = [
    "manifest_index",
    "form_type",
    "vote_type",
    "file_name",
    "file_path",
    "pdf_exists",
    "pdf_pages",
    "raw_json_pages",
    "parsed_rows",
    "status",
]


def _pdf_page_count(path: Path) -> int | None:
    if not path.exists():
        return None
    if fitz is not None:
        with fitz.open(path) as document:
            return document.page_count
    if PdfReader is not None:
        return len(PdfReader(str(path)).pages)
    return None


def _raw_ocr_dir(config: ProjectConfig, entry: ManifestEntry) -> Path:
    return config.path("raw_ocr_dir") / entry.form_type / entry.file_path.stem


def _parsed_output_path(config: ProjectConfig, entry: ManifestEntry) -> Path:
    return config.path("parsed_dir") / f"{entry.form_type}_{entry.file_path.stem}.csv"


def _count_raw_json_pages(path: Path) -> int:
    return len(list(path.glob("*.json"))) if path.exists() else 0


def _count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def _status(
    *,
    pdf_exists: bool,
    pdf_pages: int | None,
    raw_json_pages: int,
    parsed_rows: int,
) -> str:
    if not pdf_exists:
        return "missing_pdf"
    if pdf_pages is not None and raw_json_pages >= pdf_pages and parsed_rows > 0:
        return "complete"
    if raw_json_pages > 0 or parsed_rows > 0:
        return "partial"
    if pdf_pages is None:
        return "unknown_pages"
    return "not_started"


def build_ocr_progress(
    config: ProjectConfig,
    entries: list[ManifestEntry] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    source_entries = load_manifest(config) if entries is None else entries
    for manifest_index, entry in enumerate(source_entries, start=1):
        pdf_pages = _pdf_page_count(entry.file_path)
        raw_json_pages = _count_raw_json_pages(_raw_ocr_dir(config, entry))
        parsed_rows = _count_csv_rows(_parsed_output_path(config, entry))
        rows.append(
            {
                "manifest_index": manifest_index,
                "form_type": entry.form_type,
                "vote_type": entry.vote_type,
                "file_name": entry.file_path.name,
                "file_path": str(entry.file_path),
                "pdf_exists": entry.exists,
                "pdf_pages": pdf_pages if pdf_pages is not None else "",
                "raw_json_pages": raw_json_pages,
                "parsed_rows": parsed_rows,
                "status": _status(
                    pdf_exists=entry.exists,
                    pdf_pages=pdf_pages,
                    raw_json_pages=raw_json_pages,
                    parsed_rows=parsed_rows,
                ),
            }
        )
    return pd.DataFrame(rows, columns=PROGRESS_COLUMNS)


def write_ocr_progress(config: ProjectConfig) -> Path:
    config.ensure_output_dirs()
    output_path = (
        config.output("ocr_progress")
        if "ocr_progress" in config.outputs
        else config.path("processed_dir") / "ocr_progress.csv"
    )
    build_ocr_progress(config).to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Report OCR progress by manifest row.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    args = parser.parse_args()
    output_path = write_ocr_progress(load_config(args.config))
    print(output_path)


if __name__ == "__main__":
    main()
