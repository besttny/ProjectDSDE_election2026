from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

THAI_TEXT_COLUMNS = ["province", "district", "subdistrict", "choice_name", "party_name"]

LATIN_LETTER_RE = re.compile(r"[A-Za-z]")
ALLOWED_THAI_TEXT_RE = re.compile(r"^[\u0E00-\u0E7F0-9\s().,/\-]+$")


def is_empty_text(value: object) -> bool:
    return pd.isna(value) or str(value).strip() == ""


def has_invalid_thai_text_chars(value: object) -> bool:
    """Return True when a Thai text field contains non-Thai OCR noise.

    Election text fields in this project are Thai words, optional Arabic
    digits, whitespace, and light punctuation. Latin letters are strong OCR
    noise signals for names/places/parties, so they should be reviewed instead
    of silently entering the final dataset.
    """

    if is_empty_text(value):
        return False
    text = str(value).strip()
    if LATIN_LETTER_RE.search(text):
        return True
    return ALLOWED_THAI_TEXT_RE.fullmatch(text) is None


def invalid_thai_text_columns(
    row: pd.Series,
    *,
    columns: Iterable[str] = THAI_TEXT_COLUMNS,
) -> list[str]:
    return [
        column
        for column in columns
        if column in row.index and has_invalid_thai_text_chars(row[column])
    ]


def invalid_thai_text_mask(
    df: pd.DataFrame,
    *,
    columns: Iterable[str] = THAI_TEXT_COLUMNS,
) -> pd.Series:
    if df.empty:
        return pd.Series(False, index=df.index)
    mask = pd.Series(False, index=df.index)
    for column in columns:
        if column not in df.columns:
            continue
        mask |= df[column].map(has_invalid_thai_text_chars)
    return mask


def apply_thai_text_constraints(
    df: pd.DataFrame,
    *,
    columns: Iterable[str] = THAI_TEXT_COLUMNS,
) -> pd.DataFrame:
    if df.empty:
        return df
    constrained = df.copy()
    if "validation_status" not in constrained.columns:
        constrained["validation_status"] = "needs_review"
    mask = invalid_thai_text_mask(constrained, columns=columns)
    constrained.loc[mask, "validation_status"] = "needs_review"
    return constrained
