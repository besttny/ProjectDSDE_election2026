from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.pipeline.config import ProjectConfig, load_config
from src.pipeline.schema import NUMERIC_COLUMNS, RESULT_COLUMNS


def load_parsed_results(parsed_dir: Path) -> pd.DataFrame:
    files = sorted(parsed_dir.glob("*.csv"))
    if not files:
        return pd.DataFrame(columns=RESULT_COLUMNS)

    frames = []
    for path in files:
        frame = pd.read_csv(path)
        for column in RESULT_COLUMNS:
            if column not in frame.columns:
                frame[column] = pd.NA
        frames.append(frame[RESULT_COLUMNS])
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=RESULT_COLUMNS)


def normalize_results(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=RESULT_COLUMNS)

    normalized = df.copy()
    for column in RESULT_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    normalized = normalized[RESULT_COLUMNS]

    for column in NUMERIC_COLUMNS:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    string_columns = [column for column in RESULT_COLUMNS if column not in NUMERIC_COLUMNS]
    for column in string_columns:
        normalized[column] = (
            normalized[column].fillna("").astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
        )

    normalized["province"] = normalized["province"].replace("", pd.NA).ffill().fillna("")
    normalized["validation_status"] = normalized["validation_status"].replace("", "needs_review")
    return normalized


def apply_manual_corrections(df: pd.DataFrame, correction_file: Path) -> pd.DataFrame:
    if df.empty or not correction_file.exists():
        return df

    corrections = pd.read_csv(correction_file).fillna("")
    corrected = df.copy()
    required_columns = {"match_column", "match_value", "target_column", "new_value"}
    if not required_columns.issubset(corrections.columns):
        raise ValueError(f"Correction file must include {sorted(required_columns)}")

    for _, correction in corrections.iterrows():
        match_column = str(correction["match_column"]).strip()
        target_column = str(correction["target_column"]).strip()
        if not match_column or not target_column:
            continue
        if match_column not in corrected.columns or target_column not in corrected.columns:
            continue
        mask = corrected[match_column].astype(str) == str(correction["match_value"]).strip()
        corrected.loc[mask, target_column] = correction["new_value"]
    return corrected


def build_polling_station_summary(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "province",
        "constituency_no",
        "form_type",
        "vote_type",
        "polling_station_no",
        "district",
        "subdistrict",
        "total_choice_votes",
        "eligible_voters",
        "ballots_cast",
        "valid_votes",
        "invalid_votes",
        "no_vote",
        "choice_count",
        "needs_review_rows",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)

    working = df.copy()
    working["needs_review_flag"] = (working["validation_status"] != "ok").astype(int)
    summary = (
        working.groupby(
            [
                "province",
                "constituency_no",
                "form_type",
                "vote_type",
                "polling_station_no",
                "district",
                "subdistrict",
            ],
            dropna=False,
            as_index=False,
        )
        .agg(
            total_choice_votes=("votes", "sum"),
            eligible_voters=("eligible_voters", "max"),
            ballots_cast=("ballots_cast", "max"),
            valid_votes=("valid_votes", "max"),
            invalid_votes=("invalid_votes", "max"),
            no_vote=("no_vote", "max"),
            choice_count=("choice_no", "nunique"),
            needs_review_rows=("needs_review_flag", "sum"),
        )
        .sort_values(["form_type", "polling_station_no"])
    )
    return summary[columns]


def clean_results(config: ProjectConfig) -> tuple[Path, Path]:
    config.ensure_output_dirs()
    parsed = load_parsed_results(config.path("parsed_dir"))
    cleaned = normalize_results(parsed)
    cleaned = apply_manual_corrections(cleaned, config.path("correction_file"))

    results_path = config.output("election_results")
    cleaned.to_csv(results_path, index=False, encoding="utf-8-sig")

    summary = build_polling_station_summary(cleaned)
    summary_path = config.output("polling_station_summary")
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    return results_path, summary_path


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Clean parsed OCR election results.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    args = parser.parse_args()
    results_path, summary_path = clean_results(load_config(args.config))
    print(results_path)
    print(summary_path)


if __name__ == "__main__":
    main()

