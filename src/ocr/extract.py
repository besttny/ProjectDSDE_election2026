from __future__ import annotations

import argparse
import hashlib
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
from src.ocr.form_types import infer_5_18_form_type_from_texts
from src.ocr.language_filter import filter_ocr_lines_by_language
from src.ocr.parser import parse_ocr_json, rows_to_dataframe
from src.ocr.preprocess import OCRImage, preprocess_image_for_ocr, rescale_ocr_lines
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
    ocr_mode_signature: str = "",
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
        "ocr_mode_signature": ocr_mode_signature,
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


def _deep_merge(base: dict[str, Any], override: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _list_value(value: object, default: list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split(",") if part.strip()]
    return default


def _scope_ocr_profile(
    config: ProjectConfig,
    *,
    form_type: str,
    zone_name: str,
) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "primary_engine": str(config.ocr.get("primary_engine", "paddleocr")),
        "fallback_engine": str(config.ocr.get("fallback_engine", "") or ""),
        "languages": list(config.ocr.get("languages", ["th"])),
        "confidence_threshold": float(
            config.ocr.get(
                "fallback_confidence_threshold",
                config.ocr.get("confidence_threshold", 0.65),
            )
        ),
        "preprocess": "raw",
        "line_filter": "off",
    }
    profiles = config.ocr.get("profiles", {})
    if not isinstance(profiles, dict):
        return profile

    profile = _deep_merge(profile, profiles.get("default", {}))
    zone_profiles = profiles.get("zones", {})
    if isinstance(zone_profiles, dict):
        profile = _deep_merge(profile, zone_profiles.get(zone_name, {}))

    form_profiles = profiles.get("forms", {})
    if isinstance(form_profiles, dict):
        form_profile = form_profiles.get(form_type, {})
        if isinstance(form_profile, dict):
            profile = _deep_merge(profile, form_profile.get("default", {}))
            form_zone_profiles = form_profile.get("zones", {})
            if isinstance(form_zone_profiles, dict):
                profile = _deep_merge(profile, form_zone_profiles.get(zone_name, {}))
    profile["languages"] = _list_value(profile.get("languages"), ["th"])
    return profile


def _profile_form_type_for_page(entry_form_type: str, lines: list[dict[str, Any]]) -> str:
    if entry_form_type != "5_18_auto":
        return entry_form_type

    texts = [str(line.get("text", "")) for line in lines]
    return infer_5_18_form_type_from_texts(texts, default=entry_form_type)


def _preprocess_profile_options(config: ProjectConfig, profile_name: str) -> dict[str, Any]:
    preprocessing = config.ocr.get("preprocessing", {})
    if not isinstance(preprocessing, dict) or not preprocessing.get("enabled", False):
        return {"mode": "raw"}
    if profile_name in {"", "raw", "none"}:
        return {"mode": "raw"}
    profiles = preprocessing.get("profiles", {})
    if isinstance(profiles, dict) and isinstance(profiles.get(profile_name), dict):
        return dict(profiles[profile_name])
    return {"mode": profile_name}


def _profile_engine_options(
    config: ProjectConfig,
    *,
    engine_name: str,
    profile: dict[str, Any],
) -> dict[str, Any]:
    options = _engine_options(config, engine_name)
    normalized = engine_name.strip().lower()
    aliases = {"paddleocr": "paddle", "easyocr": "easyocr"}
    for key in [normalized, aliases.get(normalized, "")]:
        value = profile.get(key)
        if isinstance(value, dict):
            options = _deep_merge(options, value)
    return options


def _engine_cache_key(
    *,
    engine_name: str,
    languages: list[str],
    options: dict[str, Any],
    lazy: bool,
) -> str:
    return json.dumps(
        {
            "engine": engine_name,
            "languages": languages,
            "options": options,
            "lazy": lazy,
        },
        sort_keys=True,
        default=str,
    )


def _engine_from_cache(
    cache: dict[str, OCREngine],
    *,
    engine_name: str,
    languages: list[str],
    options: dict[str, Any],
    lazy: bool = False,
) -> OCREngine:
    key = _engine_cache_key(
        engine_name=engine_name,
        languages=languages,
        options=options,
        lazy=lazy,
    )
    if key not in cache:
        cache[key] = (
            LazyOCREngine(engine_name, languages, options)
            if lazy
            else build_engine(engine_name, languages, options)
        )
    return cache[key]


def _run_profiled_ocr(
    image_path: Path,
    *,
    config: ProjectConfig,
    profile: dict[str, Any],
    engine_cache: dict[str, OCREngine],
) -> tuple[str, list[dict[str, Any]]]:
    languages = _list_value(profile.get("languages"), ["th"])
    primary_name = str(profile.get("primary_engine") or config.ocr.get("primary_engine", "paddleocr"))
    fallback_name = str(profile.get("fallback_engine") or "").strip()
    primary_options = _profile_engine_options(config, engine_name=primary_name, profile=profile)
    fallback_options = (
        _profile_engine_options(config, engine_name=fallback_name, profile=profile)
        if fallback_name
        else {}
    )
    primary = _engine_from_cache(
        engine_cache,
        engine_name=primary_name,
        languages=languages,
        options=primary_options,
    )
    fallback = (
        _engine_from_cache(
            engine_cache,
            engine_name=fallback_name,
            languages=languages,
            options=fallback_options,
            lazy=True,
        )
        if fallback_name
        else None
    )
    return run_ocr_with_fallback(
        image_path,
        primary_engine=primary,
        fallback_engine=fallback,
        confidence_threshold=float(profile.get("confidence_threshold", 0.65)),
    )


def _prepare_ocr_input(
    image_path: Path,
    *,
    config: ProjectConfig,
    output_dir: Path,
    profile: dict[str, Any],
) -> OCRImage:
    profile_name = str(profile.get("preprocess", "raw")).strip() or "raw"
    return preprocess_image_for_ocr(
        image_path,
        output_dir=output_dir,
        profile_name=profile_name,
        options=_preprocess_profile_options(config, profile_name),
    )


def _filter_lines_for_profile(
    lines: list[dict[str, Any]],
    *,
    profile: dict[str, Any],
) -> tuple[list[dict[str, Any]], int]:
    return filter_ocr_lines_by_language(
        lines,
        mode=str(profile.get("line_filter", "off")),
    )


def _ocr_mode_signature(config: ProjectConfig) -> str:
    relevant = {
        "dpi": config.ocr.get("dpi"),
        "image_format": config.ocr.get("image_format"),
        "primary_engine": config.ocr.get("primary_engine"),
        "fallback_engine": config.ocr.get("fallback_engine"),
        "languages": config.ocr.get("languages"),
        "confidence_threshold": config.ocr.get("confidence_threshold"),
        "fallback_confidence_threshold": config.ocr.get("fallback_confidence_threshold"),
        "zone_ocr": config.ocr.get("zone_ocr"),
        "preprocessing": config.ocr.get("preprocessing"),
        "profiles": config.ocr.get("profiles"),
        "paddle": config.ocr.get("paddle"),
        "easyocr": config.ocr.get("easyocr"),
    }
    payload = json.dumps(relevant, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


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


def _raw_json_supports_current_mode(
    raw_path: Path,
    *,
    zone_ocr_enabled: bool,
    ocr_mode_signature: str,
) -> bool:
    if not raw_path.exists():
        return False
    try:
        with raw_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except json.JSONDecodeError:
        return False
    if payload.get("ocr_mode_signature") != ocr_mode_signature:
        return False
    if zone_ocr_enabled and not payload.get("zones"):
        return False
    return True


def _ocr_page(
    *,
    image_path: Path,
    entry: ManifestEntry,
    config: ProjectConfig,
    engine_cache: dict[str, OCREngine],
) -> tuple[str, list[dict[str, Any]], int, int, list[dict[str, Any]]]:
    page_width, page_height = image_size(image_path)
    full_profile = _scope_ocr_profile(
        config,
        form_type=entry.form_type,
        zone_name="full_page",
    )
    full_ocr_input = _prepare_ocr_input(
        image_path,
        config=config,
        output_dir=image_path.parent / "ocr_inputs",
        profile=full_profile,
    )
    full_engine_name, full_lines = _run_profiled_ocr(
        full_ocr_input.path,
        config=config,
        profile=full_profile,
        engine_cache=engine_cache,
    )
    full_lines = rescale_ocr_lines(full_lines, full_ocr_input)
    full_lines, full_dropped = _filter_lines_for_profile(full_lines, profile=full_profile)
    zone_profile_form_type = _profile_form_type_for_page(entry.form_type, full_lines)

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
    if full_ocr_input.profile != "raw" or full_dropped:
        zone_metadata.append(
            {
                "name": "full_page",
                "source": "rendered_page",
                "crop_box": [0, 0, page_width, page_height],
                "image_path": str(image_path),
                "ocr_image_path": str(full_ocr_input.path),
                "preprocess": full_ocr_input.profile,
                "line_filter": str(full_profile.get("line_filter", "off")),
                "dropped_line_count": full_dropped,
                "ocr_engine": full_engine_name,
                "ocr_confidence": round(average_confidence(full_lines), 4),
                "line_count": len(full_lines),
                "profile_form_type": zone_profile_form_type,
            }
        )

    for zone in zones:
        crop_path = crop_zone_image(image_path, zone, images_zone_dir)
        zone_profile = _scope_ocr_profile(
            config,
            form_type=zone_profile_form_type,
            zone_name=zone.name,
        )
        zone_ocr_input = _prepare_ocr_input(
            crop_path,
            config=config,
            output_dir=images_zone_dir / "ocr_inputs",
            profile=zone_profile,
        )
        zone_engine_name, zone_lines = _run_profiled_ocr(
            zone_ocr_input.path,
            config=config,
            profile=zone_profile,
            engine_cache=engine_cache,
        )
        zone_lines = rescale_ocr_lines(zone_lines, zone_ocr_input)
        zone_lines, dropped_lines = _filter_lines_for_profile(zone_lines, profile=zone_profile)
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
                "ocr_image_path": str(zone_ocr_input.path),
                "preprocess": zone_ocr_input.profile,
                "line_filter": str(zone_profile.get("line_filter", "off")),
                "dropped_line_count": dropped_lines,
                "ocr_engine": zone_engine_name,
                "ocr_confidence": round(average_confidence(zone_lines), 4),
                "line_count": len(zone_lines),
                "profile_form_type": zone_profile_form_type,
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

    threshold = float(config.ocr.get("confidence_threshold", 0.65))
    engine_cache: dict[str, OCREngine] = {}
    zone_ocr_enabled = _zone_ocr_enabled(config)
    mode_signature = _ocr_mode_signature(config)

    rows: list[dict[str, Any]] = []
    for image_path in images:
        raw_path = _raw_json_path(config, entry, image_path)
        if (
            not skip_existing
            or not _raw_json_supports_current_mode(
                raw_path,
                zone_ocr_enabled=zone_ocr_enabled,
                ocr_mode_signature=mode_signature,
            )
        ):
            engine_name, lines, page_width, page_height, zones = _ocr_page(
                image_path=image_path,
                entry=entry,
                config=config,
                engine_cache=engine_cache,
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
                ocr_mode_signature=mode_signature,
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
