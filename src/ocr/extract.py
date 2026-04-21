from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.ocr.engines import OCRDependencyError, build_engine, run_ocr_with_fallback
from src.ocr.parser import parse_ocr_json, rows_to_dataframe
from src.ocr.render import render_pdf_pages
from src.pipeline.config import ProjectConfig, load_config
from src.pipeline.manifest import ManifestEntry, load_manifest, missing_required_entries
from src.pipeline.schema import RESULT_COLUMNS


def _page_number_from_image(path: Path) -> int:
    stem = path.stem
    marker = "_page_"
    if marker not in stem:
        return 0
    return int(stem.rsplit(marker, 1)[1])


def _write_raw_ocr(
    *,
    output_path: Path,
    source_pdf: Path,
    source_page: int,
    ocr_engine: str,
    lines: list[dict[str, Any]],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    confidence = (
        sum(float(line.get("confidence", 0.0)) for line in lines) / len(lines)
        if lines
        else 0.0
    )
    payload = {
        "source_pdf": str(source_pdf),
        "source_page": source_page,
        "ocr_engine": ocr_engine,
        "ocr_confidence": round(confidence, 4),
        "lines": lines,
    }
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def _raw_json_path(config: ProjectConfig, entry: ManifestEntry, image_path: Path) -> Path:
    return (
        config.path("raw_ocr_dir")
        / entry.form_type
        / entry.file_path.stem
        / f"{image_path.stem}.json"
    )


def _parsed_output_path(config: ProjectConfig, entry: ManifestEntry) -> Path:
    return config.path("parsed_dir") / f"{entry.form_type}_{entry.file_path.stem}.csv"


def process_manifest_entry(
    config: ProjectConfig,
    entry: ManifestEntry,
    *,
    limit_pages: int | None = None,
    skip_existing: bool = True,
) -> Path:
    config.ensure_output_dirs()
    images_dir = config.path("raw_image_dir") / entry.form_type / entry.file_path.stem
    images = render_pdf_pages(
        entry.file_path,
        images_dir,
        dpi=int(config.ocr.get("dpi", 300)),
        image_format=str(config.ocr.get("image_format", "png")),
        limit_pages=limit_pages,
    )

    languages = list(config.ocr.get("languages", ["th", "en"]))
    threshold = float(config.ocr.get("confidence_threshold", 0.65))
    primary = build_engine(str(config.ocr.get("primary_engine", "paddleocr")), languages)
    fallback_name = str(config.ocr.get("fallback_engine", "")).strip()
    fallback = build_engine(fallback_name, languages) if fallback_name else None

    rows: list[dict[str, Any]] = []
    for image_path in images:
        raw_path = _raw_json_path(config, entry, image_path)
        if not raw_path.exists() or not skip_existing:
            engine_name, lines = run_ocr_with_fallback(
                image_path,
                primary_engine=primary,
                fallback_engine=fallback,
                confidence_threshold=threshold,
            )
            _write_raw_ocr(
                output_path=raw_path,
                source_pdf=entry.file_path,
                source_page=_page_number_from_image(image_path),
                ocr_engine=engine_name,
                lines=lines,
            )
        rows.extend(
            parse_ocr_json(
                raw_path,
                province=config.province,
                constituency_no=config.constituency_no,
                form_type=entry.form_type,
                vote_type=entry.vote_type,
                confidence_threshold=threshold,
            )
        )

    parsed_path = _parsed_output_path(config, entry)
    parsed_path.parent.mkdir(parents=True, exist_ok=True)
    rows_to_dataframe(rows).to_csv(parsed_path, index=False, encoding="utf-8-sig")
    return parsed_path


def run_extraction(
    config: ProjectConfig,
    *,
    limit_pages: int | None = None,
    skip_existing: bool = True,
) -> list[Path]:
    outputs: list[Path] = []
    entries = load_manifest(config)
    missing = missing_required_entries(entries)
    if missing:
        missing_text = "\n".join(f"- {entry.file_path}" for entry in missing)
        raise FileNotFoundError(
            "Required ECT PDF files are missing. Place files or update the manifest:\n"
            f"{missing_text}"
        )

    for entry in entries:
        if not entry.exists:
            continue
        outputs.append(
            process_manifest_entry(
                config,
                entry,
                limit_pages=limit_pages,
                skip_existing=skip_existing,
            )
        )
    if not outputs:
        empty_path = config.path("parsed_dir") / "empty_results.csv"
        empty_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=RESULT_COLUMNS).to_csv(
            empty_path, index=False, encoding="utf-8-sig"
        )
        outputs.append(empty_path)
    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Render ECT PDFs and OCR them.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    parser.add_argument("--limit-pages", type=int, default=None)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    config = load_config(args.config)
    try:
        outputs = run_extraction(
            config,
            limit_pages=args.limit_pages,
            skip_existing=not args.overwrite,
        )
    except OCRDependencyError as exc:
        raise SystemExit(str(exc)) from exc
    for output in outputs:
        print(output)


if __name__ == "__main__":
    main()

