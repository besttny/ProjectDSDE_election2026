from __future__ import annotations

import re

import pandas as pd

THAI_DIGITS = str.maketrans("๐๑๒๓๔๕๖๗๘๙", "0123456789")
DIGIT_CONFUSIONS = str.maketrans(
    {
        "O": "0",
        "o": "0",
        "Q": "0",
        "D": "0",
        "I": "1",
        "l": "1",
        "|": "1",
        "!": "1",
        "S": "5",
        "s": "5",
        "B": "8",
        "๏": "0",
    }
)


def normalize_thai_digits(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).translate(THAI_DIGITS).replace(",", "").strip()


def normalize_digit_like_text(value: object) -> str:
    return normalize_thai_digits(value).translate(DIGIT_CONFUSIONS)


def extract_first_int(text: object) -> int | None:
    match = re.search(r"\d[\d,]*", normalize_thai_digits(text))
    return int(match.group(0).replace(",", "")) if match else None


def extract_digit_cell_value(text: object, *, max_digits: int = 6) -> int | None:
    """Extract a numeric value from a crop expected to contain digits only.

    Table vote cells should be numeric. This function is intentionally stricter
    than generic integer extraction so noisy Thai/English text is not converted
    into a false vote just because it contains a stray number.
    """

    normalized = normalize_digit_like_text(text)
    if not normalized:
        return None

    compact = re.sub(r"[\s.,:;·•'\"`_~\-–—/\\()\[\]{}]+", "", normalized)
    if not compact:
        return None
    if not re.fullmatch(r"\d+", compact):
        return None
    if len(compact) > max_digits:
        return None
    return int(compact)


def extract_leading_digit_cell_value(text: object, *, max_digits: int = 4) -> int | None:
    """Extract the leading vote number from OCR text that may include Thai words.

    Some PaddleOCR vote-cell lines look like ``108Mน01`` or ``54.ห`` because the
    printed Thai spelling in parentheses is merged into the same detection box.
    The useful value is the leading digit run, while later digits usually belong
    to noisy Thai text or other cells.
    """

    normalized = normalize_thai_digits(text)
    if not normalized:
        return None
    match = re.match(r"^[\s.,:;·•'\"`_~\-–—/\\()\[\]{}]*(\d[\d\s.,:;·•'\"`_~]*)", normalized)
    if not match:
        return None
    compact = re.sub(r"\D+", "", match.group(1))
    if not compact or len(compact) > max_digits:
        return None
    if len(compact) > 1 and compact.startswith("0"):
        return None
    return int(compact)
