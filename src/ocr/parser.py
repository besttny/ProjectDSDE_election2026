from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from src.ocr.digits import extract_digit_cell_value, extract_first_int, normalize_thai_digits
from src.ocr.form_types import infer_5_18_form_type_from_texts
from src.pipeline.schema import RESULT_COLUMNS

normalize_digits = normalize_thai_digits


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


def _bbox_max_x(line: dict[str, Any]) -> float:
    bbox = line.get("bbox") or []
    return float(max(point[0] for point in bbox)) if bbox else 0.0


def _bbox_max_y(line: dict[str, Any]) -> float:
    bbox = line.get("bbox") or []
    return float(max(point[1] for point in bbox)) if bbox else 0.0


def _page_extent(
    lines: list[dict[str, Any]],
    *,
    page_width: float | None = None,
    page_height: float | None = None,
) -> tuple[float, float]:
    max_x = page_width or max((_bbox_max_x(line) for line in lines), default=1.0)
    max_y = page_height or max((_bbox_max_y(line) for line in lines), default=1.0)
    return (max(max_x, 1.0), max(max_y, 1.0))


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
        r"(?:หน่วย(?:เลือกตั้ง)?(?:ที่)?|polling\s*station)[^\d]{0,20}(\d+)",
        normalize_digits(joined),
        flags=re.IGNORECASE,
    )
    district_match = re.search(r"อำเภอ(?:/เขต)?\s*([^\s]+)", joined)
    subdistrict_match = re.search(r"ตำบล(?:/แขวง)?\s*([^\s]+)", joined)
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
    number = extract_digit_cell_value(text)
    if number is None or number < 0:
        return None
    return number


def _is_table_header_noise(text: str) -> bool:
    return any(
        keyword in text
        for keyword in [
            "จำนวนคะแนน",
            "หมายเลข",
            "ชื่อตัว",
            "ชื่อสกุล",
            "สังกัด",
            "ได้คะแนน",
            "ผู้สมัคร",
            "พรรคการเมือง",
            "ให้กรอก",
            "รวมคะแนน",
        ]
    )


def _group_table_lines(
    lines: list[dict[str, Any]],
    *,
    page_width: float | None = None,
    page_height: float | None = None,
) -> list[list[dict[str, Any]]]:
    page_width, page_height = _page_extent(
        lines, page_width=page_width, page_height=page_height
    )
    y_threshold = max(page_height * 0.028, 22.0)
    candidates = [
        line
        for line in sorted_text_lines(lines)
        if page_height * 0.60 <= _bbox_min_y(line) <= page_height * 1.01
        and not _is_noise(str(line.get("text", "")))
        and not _is_table_header_noise(str(line.get("text", "")))
    ]
    anchors = [
        line
        for line in candidates
        if (_bbox_min_x(line) / page_width) < 0.24
        and _candidate_number(str(line.get("text", ""))) is not None
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
                if (_bbox_min_x(line) / page_width) < 0.24
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
    page_width: float | None = None,
    page_height: float | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    page_width, _ = _page_extent(lines, page_width=page_width, page_height=page_height)
    for group in _group_table_lines(
        lines, page_width=page_width, page_height=page_height
    ):
        ordered = sorted(group, key=_bbox_min_x)
        number_candidates = [
            _candidate_number(str(line.get("text", "")))
            for line in ordered
            if (_bbox_min_x(line) / page_width) < 0.24
        ]
        choice_no = next((number for number in number_candidates if number is not None), None)
        if choice_no is None:
            continue

        name_tokens = [
            str(line.get("text", "")).strip()
            for line in ordered
            if 0.22 <= (_bbox_min_x(line) / page_width) < 0.44
            and not _is_noise(str(line.get("text", "")))
            and _candidate_votes(str(line.get("text", ""))) is None
        ]
        party_tokens = [
            str(line.get("text", "")).strip()
            for line in ordered
            if 0.44 <= (_bbox_min_x(line) / page_width) < 0.62
            and not _is_noise(str(line.get("text", "")))
            and _candidate_votes(str(line.get("text", ""))) is None
        ]
        name_label = " ".join(name_tokens).strip()
        party_label = " ".join(party_tokens).strip()
        if not name_label and not party_label:
            continue

        vote_candidates: list[tuple[float, int]] = []
        for line in ordered:
            x = _bbox_min_x(line)
            if (x / page_width) < 0.60:
                continue
            votes = _candidate_votes(str(line.get("text", "")))
            if votes is not None:
                vote_candidates.append((x, votes))
        votes = vote_candidates[0][1] if vote_candidates else None

        if vote_type == "party_list":
            choice_name = ""
            party_name = " ".join(part for part in [name_label, party_label] if part).strip()
        else:
            choice_name = name_label
            party_name = party_label

        rows.append(
            {
                "choice_no": choice_no,
                "choice_name": choice_name,
                "party_name": party_name,
                "votes": votes,
            }
        )
    return rows


def _lines_in_zones(lines: list[dict[str, Any]], zones: set[str]) -> list[dict[str, Any]]:
    return [line for line in lines if str(line.get("zone", "")) in zones]


def _preferred_metadata_texts(lines: list[dict[str, Any]]) -> list[str]:
    metadata_lines = _lines_in_zones(lines, {"metadata", "summary"})
    full_page_lines = _lines_in_zones(lines, {"full_page"})
    if metadata_lines:
        return line_text(metadata_lines) + line_text(full_page_lines)
    return line_text(lines)


def _preferred_choice_lines(
    lines: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    table_lines = _lines_in_zones(lines, {"table"})
    if table_lines:
        return table_lines, lines
    return lines, []


def infer_form_and_vote_type(
    texts: list[str],
    *,
    form_type: str,
    vote_type: str,
) -> tuple[str, str]:
    if form_type != "5_18_auto" and vote_type != "auto":
        return form_type, vote_type

    resolved_form_type = infer_5_18_form_type_from_texts(texts, default="5_18")
    resolved_vote_type = (
        "party_list" if resolved_form_type == "5_18_partylist" else "constituency"
    )
    return resolved_form_type, resolved_vote_type


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
    metadata = extract_metadata(_preferred_metadata_texts(lines))
    resolved_form_type, resolved_vote_type = infer_form_and_vote_type(
        texts, form_type=form_type, vote_type=vote_type
    )
    page_width = payload.get("page_width")
    page_height = payload.get("page_height")
    page_confidence = payload.get("ocr_confidence")
    if page_confidence is None and lines:
        page_confidence = sum(float(line.get("confidence", 0.0)) for line in lines) / len(lines)
    page_confidence = float(page_confidence or 0.0)
    status = "ok" if page_confidence >= confidence_threshold else "needs_review"

    choice_lines, fallback_choice_lines = _preferred_choice_lines(lines)
    choice_texts = line_text(choice_lines)
    choices = [
        choice
        for text in choice_texts
        if (choice := parse_choice_line(text)) is not None
    ]
    seen_choice_numbers = {choice["choice_no"] for choice in choices}
    for choice in parse_choice_table(
        choice_lines,
        vote_type=resolved_vote_type,
        page_width=float(page_width) if page_width else None,
        page_height=float(page_height) if page_height else None,
    ):
        if choice["choice_no"] not in seen_choice_numbers:
            choices.append(choice)
            seen_choice_numbers.add(choice["choice_no"])
    if fallback_choice_lines:
        for text in line_text(fallback_choice_lines):
            choice = parse_choice_line(text)
            if choice is not None and choice["choice_no"] not in seen_choice_numbers:
                choices.append(choice)
                seen_choice_numbers.add(choice["choice_no"])
        for choice in parse_choice_table(
            fallback_choice_lines,
            vote_type=resolved_vote_type,
            page_width=float(page_width) if page_width else None,
            page_height=float(page_height) if page_height else None,
        ):
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
