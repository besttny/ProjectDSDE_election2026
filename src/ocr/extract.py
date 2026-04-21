from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from src.ocr.engines import (
    OCRDependencyError,
    LazyOCREngine,
    OCREngine,
    average_confidence,
    build_engine,
    run_ocr_with_fallback,
)
from src.ocr.parser import parse_ocr_json, rows_to_dataframe
from src.ocr.render import render_pdf_pages
from src.ocr.zones import (
    crop_zone_image,
    detect_ocr_zones,
    image_size,
    shift_lines_to_page,
    tag_full_page_lines,
)
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
    page_width: int | None = None,
    page_height: int | None = None,
    zones: list[dict[str, Any]] | None = None,
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
        "page_width": page_width,
        "page_height": page_height,
        "zones": zones or [],
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


def _engine_options(config: ProjectConfig, engine_name: str) -> dict[str, Any]:
    normalized = engine_name.strip().lower()
    aliases = {
        "paddleocr": "paddle",
        "easyocr": "easyocr",
    }
    return dict(config.ocr.get(normalized, config.ocr.get(aliases.get(normalized, ""), {})))


def _normalize_form_filters(form_types: Sequence[str] | None) -> set[str]:
    normalized: set[str] = set()
    for value in form_types or []:
        normalized.update(part.strip() for part in value.split(",") if part.strip())
    return normalized


def _zone_ocr_options(config: ProjectConfig) -> dict[str, Any]:
    options = config.ocr.get("zone_ocr", {})
    return options if isinstance(options, dict) else {}


def _zone_ocr_enabled(config: ProjectConfig) -> bool:
    return bool(_zone_ocr_options(config).get("enabled", False))


def _raw_json_supports_current_mode(raw_path: Path, *, zone_ocr_enabled: bool) -> bool:
    if not raw_path.exists():
        return False
    if not zone_ocr_enabled:
        return True
    try:
        with raw_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except json.JSONDecodeError:
        return False
    return bool(payload.get("zones"))


def _ocr_page(
    *,
    image_path: Path,
    entry: ManifestEntry,
    config: ProjectConfig,
    primary_engine: OCREngine,
    fallback_engine: OCREngine | None,
    confidence_threshold: float,
) -> tuple[str, list[dict[str, Any]], int, int, list[dict[str, Any]]]:
    page_width, page_height = image_size(image_path)
    full_engine_name, full_lines = run_ocr_with_fallback(
        image_path,
        primary_engine=primary_engine,
        fallback_engine=fallback_engine,
        confidence_threshold=confidence_threshold,
    )

    zone_options = _zone_ocr_options(config)
    if not bool(zone_options.get("enabled", False)):
        return (
            full_engine_name,
            tag_full_page_lines(full_lines, ocr_engine=full_engine_name),
            page_width,
            page_height,
            [],
        )

    zones = detect_ocr_zones(image_path, full_lines, options=zone_options)
    images_zone_dir = (
        config.path("raw_image_dir") / entry.form_type / entry.file_path.stem / "zones"
    )
    merged_lines = tag_full_page_lines(full_lines, ocr_engine=full_engine_name)
    zone_metadata: list[dict[str, Any]] = []

    for zone in zones:
        crop_path = crop_zone_image(image_path, zone, images_zone_dir)
        zone_engine_name, zone_lines = run_ocr_with_fallback(
            crop_path,
            primary_engine=primary_engine,
            fallback_engine=fallback_engine,
            confidence_threshold=confidence_threshold,
        )
        shifted_lines = shift_lines_to_page(
            zone_lines,
            zone=zone,
            ocr_engine=zone_engine_name,
        )
        merged_lines.extend(shifted_lines)
        zone_metadata.append(
            {
                "name": zone.name,
                "source": zone.source,
                "crop_box": list(zone.crop_box),
                "image_path": str(crop_path),
                "ocr_engine": zone_engine_name,
                "ocr_confidence": round(average_confidence(zone_lines), 4),
                "line_count": len(zone_lines),
            }
        )

    return (
        f"{full_engine_name}+zones",
        merged_lines,
        page_width,
        page_height,
        zone_metadata,
    )


def select_manifest_entries(
    entries: Sequence[ManifestEntry],
    *,
    start_index: int | None = None,
    end_index: int | None = None,
    form_types: Sequence[str] | None = None,
    file_contains: str | None = None,
) -> list[ManifestEntry]:
    """Filter manifest rows for resumable Colab/local OCR batches.

    CLI indexes are 1-based and inclusive because users copy them from the
    generated OCR progress report.
    """

    if start_index is not None and start_index < 1:
        raise ValueError("--start-index must be 1 or greater")
    if end_index is not None and end_index < 1:
        raise ValueError("--end-index must be 1 or greater")
    if start_index is not None and end_index is not None and start_index > end_index:
        raise ValueError("--start-index must be less than or equal to --end-index")

    form_filter = _normalize_form_filters(form_types)
    path_filter = file_contains.casefold() if file_contains else ""
    selected: list[ManifestEntry] = []
    for manifest_index, entry in enumerate(entries, start=1):
        if start_index is not None and manifest_index < start_index:
            continue
        if end_index is not None and manifest_index > end_index:
            continue
        if form_filter and entry.form_type not in form_filter:
            continue
        if path_filter and path_filter not in str(entry.file_path).casefold():
            continue
        selected.append(entry)
    return selected


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
    fallback_threshold = float(config.ocr.get("fallback_confidence_threshold", threshold))
    primary_name = str(config.ocr.get("primary_engine", "paddleocr"))
    primary_options = _engine_options(config, primary_name)
    fallback_name = str(config.ocr.get("fallback_engine", "")).strip()
    fallback_options = _engine_options(config, fallback_name) if fallback_name else {}
    primary: OCREngine | None = None
    fallback: OCREngine | None = None
    zone_ocr_enabled = _zone_ocr_enabled(config)

    rows: list[dict[str, Any]] = []
    for image_path in images:
        raw_path = _raw_json_path(config, entry, image_path)
        if (
            not skip_existing
            or not _raw_json_supports_current_mode(
                raw_path, zone_ocr_enabled=zone_ocr_enabled
            )
        ):
            if primary is None:
                primary = build_engine(primary_name, languages, primary_options)
            if fallback_name and fallback is None:
                fallback = LazyOCREngine(fallback_name, languages, fallback_options)
            engine_name, lines, page_width, page_height, zones = _ocr_page(
                image_path=image_path,
                entry=entry,
                config=config,
                primary_engine=primary,
                fallback_engine=fallback,
                confidence_threshold=fallback_threshold,
            )
            _write_raw_ocr(
                output_path=raw_path,
                source_pdf=entry.file_path,
                source_page=_page_number_from_image(image_path),
                ocr_engine=engine_name,
                lines=lines,
                page_width=page_width,
                page_height=page_height,
                zones=zones,
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
    max_files: int | None = None,
    start_index: int | None = None,
    end_index: int | None = None,
    form_types: Sequence[str] | None = None,
    file_contains: str | None = None,
    skip_existing: bool = True,
) -> list[Path]:
    outputs: list[Path] = []
    entries = select_manifest_entries(
        load_manifest(config),
        start_index=start_index,
        end_index=end_index,
        form_types=form_types,
        file_contains=file_contains,
    )
    if not entries:
        raise ValueError("No manifest entries matched the selected OCR filters.")
    missing = missing_required_entries(entries)
    if missing:
        missing_text = "\n".join(f"- {entry.file_path}" for entry in missing)
        raise FileNotFoundError(
            "Required ECT PDF files are missing. Place files or update the manifest:\n"
            f"{missing_text}"
        )

    processed_count = 0
    for entry in entries:
        if not entry.exists:
            continue
        if max_files is not None and processed_count >= max_files:
            break
        outputs.append(
            process_manifest_entry(
                config,
                entry,
                limit_pages=limit_pages,
                skip_existing=skip_existing,
            )
        )
        processed_count += 1
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
    parser.add_argument("--max-files", type=int, default=None)
    parser.add_argument(
        "--start-index",
        type=int,
        default=None,
        help="1-based manifest row to start from; useful for Colab batch runs.",
    )
    parser.add_argument(
        "--end-index",
        type=int,
        default=None,
        help="1-based manifest row to stop at, inclusive.",
    )
    parser.add_argument(
        "--form-type",
        action="append",
        default=None,
        help="Restrict OCR to one or more form types. Repeat or comma-separate values.",
    )
    parser.add_argument(
        "--file-contains",
        default=None,
        help="Restrict OCR to manifest files whose path contains this text.",
    )
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    config = load_config(args.config)
    try:
        outputs = run_extraction(
            config,
            limit_pages=args.limit_pages,
            max_files=args.max_files,
            start_index=args.start_index,
            end_index=args.end_index,
            form_types=args.form_type,
            file_contains=args.file_contains,
            skip_existing=not args.overwrite,
        )
    except OCRDependencyError as exc:
        raise SystemExit(str(exc)) from exc
    for output in outputs:
        print(output)


if __name__ == "__main__":
    main()
