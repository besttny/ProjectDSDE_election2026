from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.pipeline.schema import RESULT_COLUMNS


def _source_name(value: object) -> str:
    text = "" if pd.isna(value) else str(value).strip()
    return Path(text).name if text else ""


def _normalize_number_key(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.notna(numeric) and float(numeric).is_integer():
        return str(int(numeric))
    return "" if pd.isna(value) else str(value).strip()


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


def apply_reviewed_vote_cells(df: pd.DataFrame, reviewed_vote_cells_file: Path) -> pd.DataFrame:
    """Apply reviewed/fallback values to individual vote cells.

    Unlike reviewed_rows.csv, this does not replace an entire source page. It is
    intended for P0 digit-cell fallback results where only one choice row needs
    a corrected vote value.
    """

    if df.empty or not reviewed_vote_cells_file.exists():
        return df

    reviewed = pd.read_csv(reviewed_vote_cells_file).fillna("")
    if reviewed.empty:
        return df

    required = {"form_type", "source_pdf", "source_page", "choice_no", "votes"}
    if not required.issubset(reviewed.columns):
        return df

    working = df.copy()
    for column in RESULT_COLUMNS:
        if column not in working.columns:
            working[column] = pd.NA
    working = working[RESULT_COLUMNS]

    source_names = working["source_pdf"].map(_source_name)
    source_pages = working["source_page"].map(_normalize_number_key)
    choice_numbers = working["choice_no"].map(_normalize_number_key)
    station_numbers = working["polling_station_no"].map(_normalize_number_key)
    form_types = working["form_type"].astype(str)

    for _, row in reviewed.iterrows():
        votes = pd.to_numeric(pd.Series([row.get("votes", "")]), errors="coerce").iloc[0]
        if pd.isna(votes):
            continue
        source_page = _normalize_number_key(row.get("source_page", ""))
        choice_no = _normalize_number_key(row.get("choice_no", ""))
        if not source_page or not choice_no:
            continue

        mask = (
            form_types.eq(str(row.get("form_type", "")))
            & source_names.eq(_source_name(row.get("source_pdf", "")))
            & source_pages.eq(source_page)
            & choice_numbers.eq(choice_no)
        )
        station_no = _normalize_number_key(row.get("polling_station_no", ""))
        if station_no:
            mask &= station_numbers.eq(station_no)
        if not mask.any():
            continue

        working.loc[mask, "votes"] = int(votes)
        working.loc[mask, "ocr_engine"] = str(row.get("ocr_engine", "") or "reviewed_vote_cell")
        confidence = pd.to_numeric(
            pd.Series([row.get("ocr_confidence", "")]),
            errors="coerce",
        ).iloc[0]
        working.loc[mask, "ocr_confidence"] = 1.0 if pd.isna(confidence) else float(confidence)
        working.loc[mask, "validation_status"] = str(
            row.get("validation_status", "") or "ok"
        )
    return working
