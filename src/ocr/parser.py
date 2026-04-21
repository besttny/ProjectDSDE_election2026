from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.schema import RESULT_COLUMNS

THAI_DIGITS = str.maketrans("๐๑๒๓๔๕๖๗๘๙", "0123456789")


def normalize_digits(value: str) -> str:
    return value.translate(THAI_DIGITS).replace(",", "")


def extract_first_int(text: str) -> int | None:
    match = re.search(r"\d[\d,]*", normalize_digits(text))
    return int(match.group(0).replace(",", "")) if match else None


def _line_sort_key(line: dict[str, Any]) -> tuple[float, float]:
    bbox = line.get("bbox") or []
    if not bbox:
        return (0.0, 0.0)
    y = min(point[1] for point in bbox)
    x = min(point[0] for point in bbox)
    return (float(y), float(x))


def sorted_text_lines(lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(lines, key=_line_sort_key)


def line_text(lines: list[dict[str, Any]]) -> list[str]:
    return [str(line.get("text", "")).strip() for line in sorted_text_lines(lines)]


def _extract_metric(texts: list[str], keywords: tuple[str, ...]) -> int | None:
    for text in texts:
        compact = normalize_digits(text)
        if any(keyword in compact for keyword in keywords):
            return extract_first_int(compact)
    return None


def extract_metadata(texts: list[str]) -> dict[str, int | str | None]:
    joined = " ".join(texts)
    station_match = re.search(
        r"(?:หน่วย(?:เลือกตั้ง)?(?:ที่)?|polling\s*station)\s*[:.]?\s*(\d+)",
        normalize_digits(joined),
        flags=re.IGNORECASE,
    )
    district_match = re.search(r"อำเภอ\s*([^\s]+)", joined)
    subdistrict_match = re.search(r"ตำบล\s*([^\s]+)", joined)
    return {
        "polling_station_no": int(station_match.group(1)) if station_match else None,
        "district": district_match.group(1).strip() if district_match else "",
        "subdistrict": subdistrict_match.group(1).strip() if subdistrict_match else "",
        "eligible_voters": _extract_metric(texts, ("ผู้มีสิทธิ", "eligible")),
        "ballots_cast": _extract_metric(texts, ("ผู้มาใช้สิทธิ", "ผู้ใช้สิทธิ", "turnout")),
        "valid_votes": _extract_metric(texts, ("บัตรดี", "valid")),
        "invalid_votes": _extract_metric(texts, ("บัตรเสีย", "invalid")),
        "no_vote": _extract_metric(texts, ("ไม่ประสงค์", "no vote")),
    }


def parse_choice_line(text: str) -> dict[str, Any] | None:
    cleaned = normalize_digits(text).strip()
    match = re.match(r"^(\d{1,3})[\s.)-]+(.+?)\s+(\d+)$", cleaned)
    if not match:
        return None

    choice_no = int(match.group(1))
    label = re.sub(r"\s+", " ", match.group(2)).strip()
    votes = int(match.group(3))

    party_name = ""
    choice_name = label
    party_match = re.search(r"\s(พรรค.+)$", label)
    if party_match:
        party_name = party_match.group(1).strip()
        choice_name = label[: party_match.start()].strip()
    elif " - " in label:
        choice_name, party_name = [part.strip() for part in label.rsplit(" - ", 1)]

    return {
        "choice_no": choice_no,
        "choice_name": choice_name,
        "party_name": party_name,
        "votes": votes,
    }


def parse_ocr_payload(
    payload: dict[str, Any],
    *,
    province: str,
    constituency_no: int,
    form_type: str,
    vote_type: str,
    confidence_threshold: float,
) -> list[dict[str, Any]]:
    lines = payload.get("lines", [])
    texts = line_text(lines)
    metadata = extract_metadata(texts)
    page_confidence = payload.get("ocr_confidence")
    if page_confidence is None and lines:
        page_confidence = sum(float(line.get("confidence", 0.0)) for line in lines) / len(lines)
    page_confidence = float(page_confidence or 0.0)
    status = "ok" if page_confidence >= confidence_threshold else "needs_review"

    rows: list[dict[str, Any]] = []
    for text in texts:
        choice = parse_choice_line(text)
        if choice is None:
            continue
        row = {column: "" for column in RESULT_COLUMNS}
        row.update(
            {
                "province": province,
                "constituency_no": constituency_no,
                "form_type": form_type,
                "vote_type": vote_type,
                "polling_station_no": metadata["polling_station_no"],
                "district": metadata["district"],
                "subdistrict": metadata["subdistrict"],
                "eligible_voters": metadata["eligible_voters"],
                "ballots_cast": metadata["ballots_cast"],
                "valid_votes": metadata["valid_votes"],
                "invalid_votes": metadata["invalid_votes"],
                "no_vote": metadata["no_vote"],
                "source_pdf": payload.get("source_pdf", ""),
                "source_page": payload.get("source_page", ""),
                "ocr_engine": payload.get("ocr_engine", ""),
                "ocr_confidence": round(page_confidence, 4),
                "validation_status": status,
            }
        )
        row.update(choice)
        rows.append(row)
    return rows


def parse_ocr_json(
    json_path: Path,
    *,
    province: str,
    constituency_no: int,
    form_type: str,
    vote_type: str,
    confidence_threshold: float,
) -> list[dict[str, Any]]:
    with json_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return parse_ocr_payload(
        payload,
        province=province,
        constituency_no=constituency_no,
        form_type=form_type,
        vote_type=vote_type,
        confidence_threshold=confidence_threshold,
    )


def rows_to_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=RESULT_COLUMNS)

