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


def _bbox_min_x(line: dict[str, Any]) -> float:
    bbox = line.get("bbox") or []
    return float(min(point[0] for point in bbox)) if bbox else 0.0


def _bbox_min_y(line: dict[str, Any]) -> float:
    bbox = line.get("bbox") or []
    return float(min(point[1] for point in bbox)) if bbox else 0.0


def sorted_text_lines(lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(lines, key=_line_sort_key)


def line_text(lines: list[dict[str, Any]]) -> list[str]:
    return [str(line.get("text", "")).strip() for line in sorted_text_lines(lines)]


def _extract_metric(texts: list[str], keywords: tuple[str, ...]) -> int | None:
    for text in texts:
        compact = normalize_digits(text)
        if "ข้อ" in compact or "แยกเป็น" in compact:
            continue
        keyword_positions = [compact.find(keyword) for keyword in keywords if keyword in compact]
        if not keyword_positions:
            continue
        first_keyword = min(keyword_positions)
        matches = list(re.finditer(r"\d[\d,]*", compact))
        after_keyword = [
            int(match.group(0).replace(",", ""))
            for match in matches
            if match.start() > first_keyword
        ]
        if after_keyword:
            return after_keyword[-1]
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


def _is_noise(text: str) -> bool:
    stripped = normalize_digits(text).strip()
    if not stripped:
        return True
    return bool(re.fullmatch(r"[.\-_/…()\s]+", stripped))


def _candidate_number(text: str) -> int | None:
    number = extract_first_int(text)
    if number is None or number <= 0 or number > 200:
        return None
    return number


def _candidate_votes(text: str) -> int | None:
    number = extract_first_int(text)
    if number is None or number < 0:
        return None
    return number


def _group_table_lines(
    lines: list[dict[str, Any]],
    y_threshold: float = 35.0,
) -> list[list[dict[str, Any]]]:
    candidates = [
        line
        for line in sorted_text_lines(lines)
        if 900 <= _bbox_min_y(line) <= 1750 and not _is_noise(str(line.get("text", "")))
    ]
    anchors = [
        line
        for line in candidates
        if _bbox_min_x(line) < 320 and _candidate_number(str(line.get("text", ""))) is not None
    ]

    groups: list[list[dict[str, Any]]] = []
    used_ids: set[int] = set()
    for anchor in anchors:
        anchor_y = _bbox_min_y(anchor)
        group: list[dict[str, Any]] = []
        anchor_number = _candidate_number(str(anchor.get("text", "")))
        for line in candidates:
            if abs(_bbox_min_y(line) - anchor_y) > y_threshold:
                continue
            line_number = (
                _candidate_number(str(line.get("text", "")))
                if _bbox_min_x(line) < 320
                else None
            )
            if line_number is not None and line_number != anchor_number:
                continue
            group.append(line)
            used_ids.add(id(line))
        if group:
            groups.append(group)

    if groups:
        return groups

    return [[line] for line in candidates if id(line) not in used_ids]


def parse_choice_table(
    lines: list[dict[str, Any]],
    *,
    vote_type: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group in _group_table_lines(lines):
        ordered = sorted(group, key=_bbox_min_x)
        number_candidates = [
            _candidate_number(str(line.get("text", "")))
            for line in ordered
            if _bbox_min_x(line) < 320
        ]
        choice_no = next((number for number in number_candidates if number is not None), None)
        if choice_no is None:
            continue

        label_tokens = [
            str(line.get("text", "")).strip()
            for line in ordered
            if 300 <= _bbox_min_x(line) < 680
            and not _is_noise(str(line.get("text", "")))
            and _candidate_votes(str(line.get("text", ""))) is None
        ]
        label = " ".join(label_tokens).strip()
        if not label:
            continue

        vote_candidates: list[tuple[float, int]] = []
        for line in ordered:
            x = _bbox_min_x(line)
            if x < 680:
                continue
            votes = _candidate_votes(str(line.get("text", "")))
            if votes is not None:
                vote_candidates.append((x, votes))
        votes = vote_candidates[0][1] if vote_candidates else None

        if vote_type == "party_list":
            choice_name = ""
            party_name = label
        else:
            choice_name = label
            party_name = ""

        rows.append(
            {
                "choice_no": choice_no,
                "choice_name": choice_name,
                "party_name": party_name,
                "votes": votes,
            }
        )
    return rows


def infer_form_and_vote_type(
    texts: list[str],
    *,
    form_type: str,
    vote_type: str,
) -> tuple[str, str]:
    if form_type != "5_18_auto" and vote_type != "auto":
        return form_type, vote_type

    joined = " ".join(texts)
    if "บัญชีรายชื่อ" in joined:
        return "5_18_partylist", "party_list"
    return "5_18", "constituency"


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
    resolved_form_type, resolved_vote_type = infer_form_and_vote_type(
        texts, form_type=form_type, vote_type=vote_type
    )
    page_confidence = payload.get("ocr_confidence")
    if page_confidence is None and lines:
        page_confidence = sum(float(line.get("confidence", 0.0)) for line in lines) / len(lines)
    page_confidence = float(page_confidence or 0.0)
    status = "ok" if page_confidence >= confidence_threshold else "needs_review"

    choices = [
        choice
        for text in texts
        if (choice := parse_choice_line(text)) is not None
    ]
    seen_choice_numbers = {choice["choice_no"] for choice in choices}
    for choice in parse_choice_table(lines, vote_type=resolved_vote_type):
        if choice["choice_no"] not in seen_choice_numbers:
            choices.append(choice)
            seen_choice_numbers.add(choice["choice_no"])

    rows: list[dict[str, Any]] = []
    for choice in choices:
        row_status = status if choice.get("votes") is not None else "needs_review"
        row = {column: "" for column in RESULT_COLUMNS}
        row.update(
            {
                "province": province,
                "constituency_no": constituency_no,
                "form_type": resolved_form_type,
                "vote_type": resolved_vote_type,
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
                "validation_status": row_status,
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
