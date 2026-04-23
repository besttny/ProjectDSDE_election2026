from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from src.ocr.digits import extract_leading_digit_cell_value
from src.pipeline.config import ProjectConfig
from src.pipeline.schema import RESULT_COLUMNS
from src.quality.master_keys import normalize_number_key, validate_choice_key

FALLBACK_FORMS = {"5_18", "5_18_partylist"}
FALLBACK_ENGINE = "raw_ocr_digit_fallback"
APPLIED_STATUSES = {"applied", "applied_zero_marker"}
AUDIT_COLUMNS = [
    "row_index",
    "form_type",
    "source_pdf",
    "source_page",
    "polling_station_no",
    "choice_no",
    "selected_votes",
    "ocr_confidence",
    "status",
    "notes",
]


def _is_missing(value: object) -> bool:
    return pd.isna(value) or str(value).strip() == ""


def _page_no(value: object) -> int | None:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return int(numeric)


def _source_stem(source_pdf: object) -> str:
    return Path(str(source_pdf)).stem


def _find_raw_ocr(config: ProjectConfig, source_pdf: object, page_no: int) -> Path | None:
    stem = _source_stem(source_pdf)
    raw_path = config.path("raw_ocr_dir").glob(f"*/{stem}/{stem}_page_{page_no:04d}.json")
    return next((path for path in sorted(raw_path) if path.is_file()), None)


def _bbox(line: dict[str, Any]) -> tuple[float, float, float, float] | None:
    points = line.get("bbox") or []
    if not points:
        return None
    xs = [float(point[0]) for point in points]
    ys = [float(point[1]) for point in points]
    return min(xs), min(ys), max(xs), max(ys)


def _boxes_overlap(
    a: tuple[float, float, float, float],
    b: tuple[float, float, float, float],
) -> bool:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    return max(ax1, bx1) < min(ax2, bx2) and max(ay1, by1) < min(ay2, by2)


def _crop_box_for_row(
    payload: dict[str, Any],
    *,
    form_type: str,
    choice_no: int,
) -> tuple[int, int, int, int] | None:
    page_width = float(payload.get("page_width") or 2480)
    page_height = float(payload.get("page_height") or 3509)
    if form_type == "5_18":
        if choice_no <= 0 or choice_no > 12:
            return None
        center_y = page_height * 0.6215 + (choice_no - 1) * page_height * 0.0246
        crop_height = max(page_height * 0.023, 64.0)
        return (
            int(round(page_width * 0.585)),
            int(round(center_y - crop_height / 2)),
            int(round(page_width * 0.665)),
            int(round(center_y + crop_height / 2)),
        )
    if form_type == "5_18_partylist":
        if choice_no <= 0 or choice_no > 57:
            return None
        if 1 <= choice_no <= 10:
            row_slot = choice_no
            center_y = page_height * 0.632 + (row_slot - 1) * page_height * 0.0299
        elif 11 <= choice_no <= 34:
            row_slot = choice_no - 10
            center_y = page_height * 0.313 + (row_slot - 1) * page_height * 0.0263
        else:
            row_slot = choice_no - 34
            center_y = page_height * 0.286 + (row_slot - 1) * page_height * 0.0264
        crop_height = max(page_height * 0.023, 64.0)
        return (
            int(round(page_width * 0.50)),
            int(round(center_y - crop_height / 2)),
            int(round(page_width * 0.635)),
            int(round(center_y + crop_height / 2)),
        )
    return None


def _unique_raw_vote_value(
    payload: dict[str, Any],
    crop_box: tuple[int, int, int, int],
    *,
    min_confidence: float,
    max_votes: int,
) -> tuple[int | None, float, str]:
    left, top, right, bottom = crop_box
    expanded_crop = (left - 8, top - 18, right + 80, bottom + 18)
    values: list[tuple[int, float]] = []
    zero_marker_confidences: list[float] = []

    for line in payload.get("lines") or []:
        bounds = _bbox(line)
        if bounds is None or not _boxes_overlap(bounds, expanded_crop):
            continue
        _, line_top, _, line_bottom = bounds
        line_center_y = (line_top + line_bottom) / 2
        if not (top - 12 <= line_center_y <= bottom + 12):
            continue

        try:
            confidence = float(line.get("confidence", 0.0))
        except (TypeError, ValueError):
            confidence = 0.0
        text = str(line.get("text", ""))
        value = extract_leading_digit_cell_value(text, max_digits=4)
        if value is None:
            compact = re.sub(r"\s+", "", text)
            if compact and re.fullmatch(r"[\-–—_.·•:]+", compact):
                zero_marker_confidences.append(confidence)
            continue
        values.append((value, confidence))

    if not values:
        high_conf_zero_markers = [
            confidence for confidence in zero_marker_confidences if confidence >= min_confidence
        ]
        if high_conf_zero_markers:
            return 0, max(high_conf_zero_markers), "applied_zero_marker"
        return None, 0.0, "blank"

    high_conf_values = [(value, conf) for value, conf in values if conf >= min_confidence]
    if not high_conf_values:
        return None, max(conf for _, conf in values), "low_confidence"

    unique_values = {value for value, _ in high_conf_values}
    if len(unique_values) != 1:
        return None, max(conf for _, conf in high_conf_values), "conflict"

    selected = next(iter(unique_values))
    if selected > max_votes:
        return None, max(conf for _, conf in high_conf_values), "out_of_range"
    return selected, max(conf for value, conf in high_conf_values if value == selected), "applied"


def build_raw_digit_vote_fallbacks(
    df: pd.DataFrame,
    config: ProjectConfig,
    *,
    min_confidence: float | None = None,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=AUDIT_COLUMNS)

    threshold = (
        float(min_confidence)
        if min_confidence is not None
        else float(config.quality.get("auto_digit_fallback_min_confidence", 0.75))
    )
    max_votes = int(config.quality.get("auto_digit_fallback_max_votes", 999))
    raw_cache: dict[Path, dict[str, Any]] = {}
    records: list[dict[str, object]] = []

    for index, row in df.iterrows():
        form_type = str(row.get("form_type", "")).strip()
        if form_type not in FALLBACK_FORMS or not _is_missing(row.get("votes", "")):
            continue

        source_page = _page_no(row.get("source_page", ""))
        choice_key = normalize_number_key(row.get("choice_no", ""))
        if source_page is None or not choice_key:
            continue

        choice_no = int(choice_key)
        base = {
            "row_index": index,
            "form_type": form_type,
            "source_pdf": row.get("source_pdf", ""),
            "source_page": source_page,
            "polling_station_no": row.get("polling_station_no", ""),
            "choice_no": choice_no,
            "selected_votes": "",
            "ocr_confidence": "",
        }
        if (
            validate_choice_key(
                config,
                form_type=form_type,
                choice_no=choice_no,
                province=row.get("province", ""),
                constituency_no=row.get("constituency_no", ""),
            )
            != "valid"
        ):
            records.append({**base, "status": "invalid_choice_no", "notes": "Choice is not in the official master."})
            continue

        raw_path = _find_raw_ocr(config, row.get("source_pdf", ""), source_page)
        if raw_path is None:
            records.append({**base, "status": "missing_raw_ocr", "notes": "Raw OCR JSON was not found for this source page."})
            continue
        if raw_path not in raw_cache:
            try:
                raw_cache[raw_path] = json.loads(raw_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                records.append({**base, "status": "invalid_raw_ocr", "notes": f"Could not read {raw_path}."})
                continue

        payload = raw_cache[raw_path]
        crop_box = _crop_box_for_row(payload, form_type=form_type, choice_no=choice_no)
        if crop_box is None:
            records.append({**base, "status": "missing_template_crop", "notes": "No fixed-layout crop is available for this row."})
            continue

        selected, confidence, status = _unique_raw_vote_value(
            payload,
            crop_box,
            min_confidence=threshold,
            max_votes=max_votes,
        )
        records.append(
            {
                **base,
                "selected_votes": "" if selected is None else selected,
                "ocr_confidence": "" if confidence == 0.0 else round(confidence, 4),
                "status": status,
                "notes": (
                    "Applied unique high-confidence raw OCR value from the vote-cell template."
                    if status == "applied"
                    else "Applied high-confidence zero marker from the vote-cell template."
                    if status == "applied_zero_marker"
                    else "Not applied automatically; requires crop/manual review."
                ),
            }
        )

    return pd.DataFrame(records, columns=AUDIT_COLUMNS)


def apply_raw_digit_vote_fallback(
    df: pd.DataFrame,
    config: ProjectConfig,
    *,
    min_confidence: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df, pd.DataFrame(columns=AUDIT_COLUMNS)

    audit = build_raw_digit_vote_fallbacks(
        df,
        config,
        min_confidence=min_confidence,
    )
    if audit.empty:
        return df, audit

    filled = df.copy()
    for _, row in audit[audit["status"].isin(APPLIED_STATUSES)].iterrows():
        index = int(row["row_index"])
        if index not in filled.index:
            continue
        if not _is_missing(filled.at[index, "votes"]):
            continue
        filled.at[index, "votes"] = str(int(row["selected_votes"]))
        filled.at[index, "ocr_engine"] = FALLBACK_ENGINE
        filled.at[index, "ocr_confidence"] = str(float(row["ocr_confidence"]))
        filled.at[index, "validation_status"] = "ok"
    return filled[RESULT_COLUMNS], audit
