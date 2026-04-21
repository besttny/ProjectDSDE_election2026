from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.pipeline.clean import clean_results
from src.pipeline.config import ProjectConfig, load_config
from src.pipeline.schema import RESULT_COLUMNS
from src.quality.master_match import build_master_match_report


REVIEW_COLUMNS = [
    "priority",
    "reason",
    "suggested_action",
    "row_index",
    "province",
    "form_type",
    "vote_type",
    "polling_station_no",
    "choice_no",
    "choice_name",
    "party_name",
    "votes",
    "ballots_cast",
    "valid_votes",
    "invalid_votes",
    "no_vote",
    "ocr_confidence",
    "validation_status",
    "source_pdf",
    "source_page",
]


REASON_ACTIONS = {
    "missing_votes": "Open source PDF page and type the vote value manually.",
    "missing_station_id": "Read station id from PDF header or station list.",
    "missing_choice_no": "Check table row number against candidate or party master list.",
    "negative_votes": "Recheck OCR/parsing because votes cannot be negative.",
    "low_ocr_confidence": "Review the raw image/PDF crop before accepting this row.",
    "parser_marked_needs_review": "Resolve parser/OCR uncertainty and add manual correction if repeatable.",
    "duplicate_choice_row": "Keep one canonical row for this form/station/choice key.",
    "choice_votes_exceed_valid_votes": "Check all vote rows in this station/form group.",
    "ballot_accounting_mismatch": "Verify total, invalid, and no-vote ballot fields from the summary box.",
    "master_data_unmatched": "Confirm spelling and add alias or correction in external master/correction files.",
}


def _read_results(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=RESULT_COLUMNS)
    return pd.read_csv(path)


def _is_missing(series: pd.Series) -> pd.Series:
    return series.isna() | (series.astype(str).str.strip() == "")


def _priority(reason: str) -> str:
    if reason in {
        "missing_votes",
        "missing_station_id",
        "missing_choice_no",
        "negative_votes",
        "duplicate_choice_row",
        "choice_votes_exceed_valid_votes",
        "ballot_accounting_mismatch",
    }:
        return "P0"
    if reason in {"low_ocr_confidence", "parser_marked_needs_review", "master_data_unmatched"}:
        return "P1"
    return "P2"


def _make_review_rows(df: pd.DataFrame, indexes: Iterable[int], reason: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in sorted(set(int(value) for value in indexes)):
        record = df.loc[index]
        row = {column: record.get(column, "") for column in REVIEW_COLUMNS if column not in {"priority", "reason", "suggested_action", "row_index"}}
        row.update(
            {
                "priority": _priority(reason),
                "reason": reason,
                "suggested_action": REASON_ACTIONS.get(reason, "Review this row against source evidence."),
                "row_index": index,
            }
        )
        rows.append(row)
    return rows


def _duplicate_indexes(df: pd.DataFrame) -> list[int]:
    subset = ["form_type", "polling_station_no", "choice_no"]
    if df.empty or not set(subset).issubset(df.columns):
        return []
    base = df.dropna(subset=["polling_station_no", "choice_no"])
    duplicated = base.duplicated(subset=subset, keep=False)
    return base.index[duplicated].tolist()


def _exceeded_valid_vote_indexes(df: pd.DataFrame) -> list[int]:
    required = {"form_type", "polling_station_no", "votes", "valid_votes"}
    if df.empty or not required.issubset(df.columns):
        return []
    base = df.dropna(subset=["polling_station_no", "valid_votes"]).copy()
    if base.empty:
        return []
    base["votes"] = pd.to_numeric(base["votes"], errors="coerce").fillna(0)
    base["valid_votes"] = pd.to_numeric(base["valid_votes"], errors="coerce")
    grouped = base.groupby(["form_type", "polling_station_no"], dropna=False).agg(
        total_votes=("votes", "sum"),
        valid_votes=("valid_votes", "max"),
    )
    exceeded_keys = {
        key
        for key, row in grouped.iterrows()
        if pd.notna(row["valid_votes"]) and row["total_votes"] > row["valid_votes"]
    }
    if not exceeded_keys:
        return []
    keys = list(zip(base["form_type"], base["polling_station_no"], strict=False))
    return [index for index, key in zip(base.index, keys, strict=False) if key in exceeded_keys]


def _ballot_accounting_mismatch_indexes(df: pd.DataFrame) -> list[int]:
    required = {"ballots_cast", "valid_votes", "invalid_votes", "no_vote"}
    if df.empty or not required.issubset(df.columns):
        return []
    values = df[list(required)].apply(pd.to_numeric, errors="coerce")
    complete = values.notna().all(axis=1)
    expected = values["valid_votes"] + values["invalid_votes"] + values["no_vote"]
    mismatch = complete & (expected != values["ballots_cast"])
    return df.index[mismatch].tolist()


def _master_unmatched_indexes(config: ProjectConfig) -> list[int]:
    report = build_master_match_report(config)
    if report.empty or "status" not in report.columns:
        return []
    unmatched = report[report["status"].isin(["suggested", "no_confident_match"])]
    if unmatched.empty:
        return []
    results_path = config.output("election_results")
    df = _read_results(results_path).reset_index().rename(columns={"index": "row_index"})
    keys = ["form_type", "polling_station_no", "choice_no", "source_pdf", "source_page"]
    merged = df.merge(unmatched[keys].drop_duplicates(), on=keys, how="inner")
    return merged["row_index"].dropna().astype(int).tolist()


def build_review_queue(config: ProjectConfig) -> pd.DataFrame:
    results_path = config.output("election_results")
    if not results_path.exists():
        clean_results(config)
    df = _read_results(results_path)
    if df.empty:
        return pd.DataFrame(columns=REVIEW_COLUMNS)

    working = df.copy()
    for column in RESULT_COLUMNS:
        if column not in working.columns:
            working[column] = pd.NA
    working = working[RESULT_COLUMNS]

    reasons: list[tuple[str, list[int]]] = []
    reasons.append(("missing_votes", working.index[_is_missing(working["votes"])].tolist()))
    election_day = working["form_type"].astype(str).isin(["5_18", "5_18_partylist", "5_18_auto"])
    missing_station = election_day & _is_missing(working["polling_station_no"])
    reasons.append(("missing_station_id", working.index[missing_station].tolist()))
    reasons.append(("missing_choice_no", working.index[_is_missing(working["choice_no"])].tolist()))

    votes = pd.to_numeric(working["votes"], errors="coerce")
    reasons.append(("negative_votes", working.index[votes < 0].tolist()))

    confidence_threshold = float(config.ocr.get("confidence_threshold", 0.65))
    confidence = pd.to_numeric(working["ocr_confidence"], errors="coerce")
    reasons.append(("low_ocr_confidence", working.index[confidence < confidence_threshold].tolist()))

    reasons.append(
        (
            "parser_marked_needs_review",
            working.index[working["validation_status"].astype(str) != "ok"].tolist(),
        )
    )
    reasons.append(("duplicate_choice_row", _duplicate_indexes(working)))
    reasons.append(("choice_votes_exceed_valid_votes", _exceeded_valid_vote_indexes(working)))
    reasons.append(("ballot_accounting_mismatch", _ballot_accounting_mismatch_indexes(working)))
    reasons.append(("master_data_unmatched", _master_unmatched_indexes(config)))

    rows: list[dict[str, object]] = []
    for reason, indexes in reasons:
        rows.extend(_make_review_rows(working, indexes, reason))

    if not rows:
        return pd.DataFrame(columns=REVIEW_COLUMNS)
    queue = pd.DataFrame(rows, columns=REVIEW_COLUMNS)
    priority_order = {"P0": 0, "P1": 1, "P2": 2}
    queue["_priority_order"] = queue["priority"].map(priority_order).fillna(99)
    queue = queue.sort_values(["_priority_order", "source_pdf", "source_page", "row_index", "reason"])
    return queue.drop(columns=["_priority_order"]).reset_index(drop=True)


def write_review_queue(config: ProjectConfig) -> Path:
    config.ensure_output_dirs()
    output_path = config.output("review_queue")
    build_review_queue(config).to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build OCR/manual-review queue.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    args = parser.parse_args()
    print(write_review_queue(load_config(args.config)))


if __name__ == "__main__":
    main()
