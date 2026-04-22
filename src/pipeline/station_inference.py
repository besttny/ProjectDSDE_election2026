from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.config import ProjectConfig
from src.pipeline.manifest import ManifestEntry, load_manifest

STATION_FORMS = {"5_18", "5_18_partylist"}

PARTY_HEADER_MARKERS = (
    "แบบบัญชีรายชื่อ",
    "แบบัญชีรายชื่อ",
    "๕/๑๘(บช)",
    "๕/๑๘ (บช)",
    "๕/18(บช)",
    "๕/18 (บช)",
    "๕๑๘ (บช)",
    "518 (บช)",
    "5/18 (บช)",
)


def _page_number(path: Path) -> int:
    match = re.search(r"_page_(\d+)", path.stem)
    return int(match.group(1)) if match else 0


def _read_header_text(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    lines = payload.get("lines", [])
    return " ".join(str(line.get("text", "")) for line in lines[:18])


def _is_party_header(text: str) -> bool:
    return any(marker in text for marker in PARTY_HEADER_MARKERS)


def _first_party_page(raw_dir: Path) -> int | None:
    for path in sorted(raw_dir.glob("*.json"), key=_page_number):
        if _is_party_header(_read_header_text(path)):
            return _page_number(path)
    return None


def _raw_dir(config: ProjectConfig, entry: ManifestEntry) -> Path:
    return config.path("raw_ocr_dir") / entry.form_type / entry.file_path.stem


def _source_name(value: object) -> str:
    text = "" if pd.isna(value) else str(value).strip()
    return Path(text).name if text else ""


def _clean_subdistrict_from_stem(stem: str) -> str:
    name = re.split(r"-|_", stem, maxsplit=1)[0].strip()
    name = re.sub(r"^(ต\.|ทต\.)", "", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name


def _district_from_path(path: Path) -> str:
    for part in reversed(path.parts):
        if part.startswith("อำเภอ"):
            return part.replace("อำเภอ", "", 1).strip()
    return ""


def _page_assignments(entry: ManifestEntry, raw_dir: Path) -> dict[tuple[str, int], int]:
    pages = [_page_number(path) for path in raw_dir.glob("*.json")]
    if not pages:
        return {}
    max_page = max(pages)
    assignments: dict[tuple[str, int], int] = {}

    if entry.form_type == "5_18":
        for page in pages:
            assignments[("5_18", page)] = math.ceil(page / 2)
        return assignments

    if entry.form_type == "5_18_partylist":
        for page in pages:
            assignments[("5_18_partylist", page)] = math.ceil(page / 4)
        return assignments

    if entry.form_type != "5_18_auto":
        return assignments

    first_party = _first_party_page(raw_dir)
    if first_party is not None and first_party <= 4:
        for page in pages:
            local_station = math.ceil(page / 6)
            if ((page - 1) % 6) + 1 <= 2:
                assignments[("5_18", page)] = local_station
            else:
                assignments[("5_18_partylist", page)] = local_station
        return assignments

    if first_party is not None:
        for page in pages:
            if page < first_party:
                assignments[("5_18", page)] = math.ceil(page / 2)
            else:
                assignments[("5_18_partylist", page)] = math.ceil((page - first_party + 1) / 4)
        return assignments

    for page in pages:
        assignments[("5_18", page)] = math.ceil(page / 2)
    return assignments


def build_station_page_map(config: ProjectConfig) -> dict[tuple[str, str, int], dict[str, Any]]:
    """Infer stable station ids from source PDF page position.

    ECT 5/18 files are fixed-layout documents. OCR often reads the printed
    station number as a local "1" or misses it entirely, so the downstream
    schema uses a deterministic station id inferred from manifest order,
    source PDF, and page group instead of trusting OCR text.
    """

    station_offsets = {"5_18": 0, "5_18_partylist": 0}
    page_map: dict[tuple[str, str, int], dict[str, Any]] = {}
    for entry in load_manifest(config):
        raw_dir = _raw_dir(config, entry)
        assignments = _page_assignments(entry, raw_dir)
        if not assignments:
            continue

        max_local_by_form = {
            form_type: max(
                local_station
                for (assigned_form, _), local_station in assignments.items()
                if assigned_form == form_type
            )
            for form_type in STATION_FORMS
            if any(assigned_form == form_type for assigned_form, _ in assignments)
        }
        source_name = entry.file_path.name
        district = _district_from_path(entry.file_path)
        subdistrict = _clean_subdistrict_from_stem(entry.file_path.stem)

        for (form_type, page), local_station in assignments.items():
            global_station = station_offsets[form_type] + local_station
            page_map[(form_type, source_name, page)] = {
                "polling_station_no": global_station,
                "district": district,
                "subdistrict": subdistrict,
                "station_group_local": local_station,
            }

        for form_type, max_local in max_local_by_form.items():
            station_offsets[form_type] += max_local

    return page_map


def apply_station_inference(df: pd.DataFrame, config: ProjectConfig) -> pd.DataFrame:
    if df.empty:
        return df
    page_map = build_station_page_map(config)
    if not page_map:
        return df

    corrected = df.copy()
    for index, row in corrected.iterrows():
        form_type = str(row.get("form_type", "")).strip()
        if form_type not in STATION_FORMS:
            continue
        source_page = pd.to_numeric(pd.Series([row.get("source_page", "")]), errors="coerce").iloc[0]
        if pd.isna(source_page):
            continue
        key = (form_type, _source_name(row.get("source_pdf", "")), int(source_page))
        inferred = page_map.get(key)
        if not inferred:
            continue
        corrected.at[index, "polling_station_no"] = inferred["polling_station_no"]
        if inferred["district"]:
            corrected.at[index, "district"] = inferred["district"]
        if inferred["subdistrict"]:
            corrected.at[index, "subdistrict"] = inferred["subdistrict"]
    return corrected
