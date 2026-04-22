from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.pipeline.config import ProjectConfig, load_config
from src.quality.master_keys import normalize_number_key, validate_choice_key
from src.quality.review_queue import write_review_queue

REVIEW_COLUMNS = [
    "row_index",
    "priority",
    "form_type",
    "vote_type",
    "polling_station_no",
    "choice_no",
    "choice_name",
    "party_name",
    "votes",
    "source_pdf",
    "source_page",
    "choice_in_master",
    "has_missing_votes",
    "classification",
    "recommended_action",
]


def _normalize_number(value: object) -> str:
    return normalize_number_key(value)


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _is_missing(value: object) -> bool:
    return pd.isna(value) or str(value).strip() == ""


def build_invalid_text_review(config: ProjectConfig) -> pd.DataFrame:
    review_queue_path = config.output("review_queue")
    if not review_queue_path.exists():
        write_review_queue(config)
    queue = _read_csv(review_queue_path)
    if queue.empty or "reason" not in queue.columns:
        return pd.DataFrame(columns=REVIEW_COLUMNS)

    rows = queue[queue["reason"].astype(str).eq("invalid_text_charset")].copy()
    if rows.empty:
        return pd.DataFrame(columns=REVIEW_COLUMNS)

    records: list[dict[str, object]] = []
    for _, row in rows.iterrows():
        form_type = str(row.get("form_type", "")).strip()
        choice_no = _normalize_number(row.get("choice_no", ""))
        choice_validation = validate_choice_key(
            config,
            form_type=form_type,
            choice_no=choice_no,
            province=row.get("province", ""),
            constituency_no=row.get("constituency_no", ""),
        )
        choice_in_master = choice_validation == "valid"
        has_missing_votes = _is_missing(row.get("votes", ""))
        if choice_validation == "invalid":
            classification = "invalid_choice_number"
            recommended_action = (
                "Do not add this choice number to master automatically. Verify the "
                "source row because OCR likely merged table text or misread row number."
            )
        elif has_missing_votes:
            classification = "needs_digit_fallback"
            recommended_action = (
                "Create a vote-cell crop or use Google fallback, then record the "
                "correct vote in data/external/reviewed_vote_cells.csv."
            )
        else:
            classification = "master_can_fill_name"
            recommended_action = (
                "Keep official master as source of candidate/party text; rerun "
                "run_all --skip-ocr after any manual vote corrections."
            )
        records.append(
            {
                "row_index": row.get("row_index", ""),
                "priority": row.get("priority", ""),
                "form_type": form_type,
                "vote_type": row.get("vote_type", ""),
                "polling_station_no": row.get("polling_station_no", ""),
                "choice_no": row.get("choice_no", ""),
                "choice_name": row.get("choice_name", ""),
                "party_name": row.get("party_name", ""),
                "votes": row.get("votes", ""),
                "source_pdf": row.get("source_pdf", ""),
                "source_page": row.get("source_page", ""),
                "choice_in_master": choice_in_master,
                "has_missing_votes": has_missing_votes,
                "classification": classification,
                "recommended_action": recommended_action,
            }
        )
    return pd.DataFrame(records, columns=REVIEW_COLUMNS)


def write_invalid_text_review(config: ProjectConfig) -> tuple[Path, Path]:
    config.ensure_output_dirs()
    review = build_invalid_text_review(config)
    review_path = (
        config.output("invalid_text_review")
        if "invalid_text_review" in config.outputs
        else config.path("processed_dir") / "invalid_text_charset_review.csv"
    )
    missing_vote_path = (
        config.output("invalid_text_missing_vote_targets")
        if "invalid_text_missing_vote_targets" in config.outputs
        else config.path("processed_dir") / "invalid_text_missing_vote_targets.csv"
    )
    review.to_csv(review_path, index=False, encoding="utf-8-sig")
    needs_digit_fallback = review["classification"].astype(str).eq("needs_digit_fallback")
    review[needs_digit_fallback].to_csv(
        missing_vote_path,
        index=False,
        encoding="utf-8-sig",
    )
    return review_path, missing_vote_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize invalid Thai text OCR rows.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    args = parser.parse_args()
    for path in write_invalid_text_review(load_config(args.config)):
        print(path)


if __name__ == "__main__":
    main()
