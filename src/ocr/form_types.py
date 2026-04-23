from __future__ import annotations

import re

from src.ocr.digits import normalize_thai_digits

OFFICIAL_OCR_FORM_TYPES = (
    "5_16",
    "5_16_partylist",
    "5_17",
    "5_17_partylist",
    "5_18",
    "5_18_partylist",
)

CONSTITUENCY_FORM_TYPES = {"5_16", "5_17", "5_18"}
PARTYLIST_FORM_TYPES = {"5_16_partylist", "5_17_partylist", "5_18_partylist"}


def _compact_text(text: str) -> str:
    return re.sub(r"\s+", "", normalize_thai_digits(text))


def infer_5_18_form_type_from_texts(
    texts: list[str],
    *,
    default: str = "5_18",
) -> str:
    """Infer the real document type for mixed 5/18 PDFs from OCR header text."""

    header = " ".join(str(text) for text in texts[:18])
    compact = _compact_text(header)
    partylist_markers = (
        "แบบบัญชีรายชื่อ",
        "บัญชีรายชื่อส.ส",
        "บัญชีรายชื่อสมาชิก",
        "คะแนนบัญชีรายชื่อ",
        "(บช",
        "บช)",
        "บช.",
    )
    if any(marker in header or marker in compact for marker in partylist_markers):
        return "5_18_partylist"
    if "แบ่งเขต" in header or "แบ่งเขต" in compact:
        return "5_18"
    if (
        "บัญชีรายชื่อ" in header
        and "ผู้มีสิทธิ" not in header
        and ("พรรคการเมือง" in header or "พรรค" in header)
    ):
        return "5_18_partylist"
    return default
