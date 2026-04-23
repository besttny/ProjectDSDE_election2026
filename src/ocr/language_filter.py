from __future__ import annotations

import re
from typing import Any

THAI_OR_DIGIT_RE = re.compile(r"[\u0E00-\u0E7F0-9๐-๙]")
THAI_RE = re.compile(r"[\u0E00-\u0E7F]")
DIGIT_RE = re.compile(r"[0-9๐-๙]")


def _line_allowed(text: str, mode: str) -> bool:
    mode = mode.strip().lower()
    if mode in {"", "off", "none", "raw"}:
        return True
    if mode == "thai_numeric":
        return bool(THAI_OR_DIGIT_RE.search(text))
    if mode == "thai":
        return bool(THAI_RE.search(text))
    if mode == "digits":
        return bool(DIGIT_RE.search(text))
    return True


def filter_ocr_lines_by_language(
    lines: list[dict[str, Any]],
    *,
    mode: str,
) -> tuple[list[dict[str, Any]], int]:
    kept: list[dict[str, Any]] = []
    dropped = 0
    for line in lines:
        text = str(line.get("text", "")).strip()
        if _line_allowed(text, mode):
            kept.append(line)
        else:
            dropped += 1
    return kept, dropped
