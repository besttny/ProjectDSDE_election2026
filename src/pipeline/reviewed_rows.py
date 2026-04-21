from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.pipeline.schema import RESULT_COLUMNS


def _source_name(value: object) -> str:
    text = "" if pd.isna(value) else str(value).strip()
    return Path(text).name if text else ""


def _normalize_reviewed_rows(reviewed: pd.DataFrame) -> pd.DataFrame:
    normalized = reviewed.copy()
    for column in RESULT_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    return normalized[RESULT_COLUMNS]


def apply_reviewed_rows(df: pd.DataFrame, reviewed_rows_file: Path) -> pd.DataFrame:
    """Replace OCR rows with manually reviewed rows for the same source page.

    This is intentionally source-page scoped. If a reviewed row exists for
    source PDF page X and form Y, OCR rows from that same source page/form are
    removed before the reviewed rows are appended.
    """

    if not reviewed_rows_file.exists():
        return df

    reviewed_raw = pd.read_csv(reviewed_rows_file).fillna("")
    if reviewed_raw.empty:
        return df

    reviewed = _normalize_reviewed_rows(reviewed_raw)
    if df.empty:
        return reviewed

    working = df.copy()
    for column in RESULT_COLUMNS:
        if column not in working.columns:
            working[column] = pd.NA
    working = working[RESULT_COLUMNS]

    reviewed_keys = {
        (
            str(row["form_type"]),
            _source_name(row["source_pdf"]),
            str(row["source_page"]),
        )
        for _, row in reviewed.iterrows()
    }

    working_keys = pd.Series(
        [
            (
                str(row["form_type"]),
                _source_name(row["source_pdf"]),
                str(row["source_page"]),
            )
            for _, row in working.iterrows()
        ],
        index=working.index,
    )
    filtered = working[~working_keys.isin(reviewed_keys)]
    return pd.concat([filtered, reviewed], ignore_index=True)[RESULT_COLUMNS]
