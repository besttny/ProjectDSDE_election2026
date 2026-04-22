from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.pipeline.config import ProjectConfig, load_config
from src.pipeline.reviewed_rows import apply_reviewed_rows
from src.pipeline.schema import NUMERIC_COLUMNS, RESULT_COLUMNS
from src.pipeline.station_inference import apply_station_inference

CONSTITUENCY_FORMS = {"5_16", "5_17", "5_18"}
PARTYLIST_FORMS = {"5_16_partylist", "5_17_partylist", "5_18_partylist"}


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


def _normalize_key_number(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.notna(numeric) and float(numeric).is_integer():
        return str(int(numeric))
    return "" if pd.isna(value) else str(value).strip()


def _normalize_key_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).replace("\ufeff", "").strip()


def _candidate_master_map(path: Path) -> dict[tuple[str, str, str], dict[str, str]]:
    if not path.exists():
        return {}
    frame = pd.read_csv(path).fillna("")
    required = {"province", "constituency_no", "candidate_no", "canonical_name", "party_name"}
    if not required.issubset(frame.columns):
        return {}
    mapping: dict[tuple[str, str, str], dict[str, str]] = {}
    for _, row in frame.iterrows():
        province = _normalize_key_text(row["province"])
        constituency_no = _normalize_key_number(row["constituency_no"])
        choice_no = _normalize_key_number(row["candidate_no"])
        if not province or not constituency_no or not choice_no:
            continue
        mapping[(province, constituency_no, choice_no)] = {
            "choice_name": str(row["canonical_name"]).strip(),
            "party_name": str(row["party_name"]).strip(),
        }
    return mapping


def _party_master_map(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    frame = pd.read_csv(path).fillna("")
    if not {"party_no", "canonical_name"}.issubset(frame.columns):
        return {}
    mapping: dict[str, str] = {}
    for _, row in frame.iterrows():
        party_no = _normalize_key_number(row["party_no"])
        party_name = str(row["canonical_name"]).strip()
        if party_no and party_name:
            mapping[party_no] = party_name
    return mapping


def _candidate_master_keys(path: Path) -> set[tuple[str, str, str]]:
    return set(_candidate_master_map(path).keys())


def _party_master_keys(path: Path) -> set[str]:
    return set(_party_master_map(path).keys())


def _candidate_result_key(row: pd.Series, config: ProjectConfig) -> tuple[str, str, str]:
    province = _normalize_key_text(row.get("province", "")) or _normalize_key_text(config.province)
    constituency_no = (
        _normalize_key_number(row.get("constituency_no", ""))
        or _normalize_key_number(config.constituency_no)
    )
    choice_no = _normalize_key_number(row.get("choice_no", ""))
    return province, constituency_no, choice_no


def apply_master_names(df: pd.DataFrame, config: ProjectConfig) -> pd.DataFrame:
    if df.empty:
        return df

    candidate_map = _candidate_master_map(config.path("master_candidates_file"))
    party_map = _party_master_map(config.path("master_parties_file"))
    if not candidate_map and not party_map:
        return df

    filled = df.copy()
    for index, row in filled.iterrows():
        form_type = str(row.get("form_type", "")).strip()
        choice_no = _normalize_key_number(row.get("choice_no", ""))
        if not choice_no:
            continue
        if form_type in CONSTITUENCY_FORMS:
            candidate = candidate_map.get(_candidate_result_key(row, config))
            if candidate:
                if candidate["choice_name"]:
                    filled.at[index, "choice_name"] = candidate["choice_name"]
                if candidate["party_name"]:
                    filled.at[index, "party_name"] = candidate["party_name"]
        elif form_type in PARTYLIST_FORMS:
            party_name = party_map.get(choice_no)
            if party_name:
                filled.at[index, "choice_name"] = ""
                filled.at[index, "party_name"] = party_name
    return filled


def apply_master_key_validation(df: pd.DataFrame, config: ProjectConfig) -> pd.DataFrame:
    if df.empty:
        return df

    candidate_keys = _candidate_master_keys(config.path("master_candidates_file"))
    party_keys = _party_master_keys(config.path("master_parties_file"))
    if not candidate_keys and not party_keys:
        return df

    validated = df.copy()
    for index, row in validated.iterrows():
        form_type = str(row.get("form_type", "")).strip()
        choice_no = _normalize_key_number(row.get("choice_no", ""))
        if not choice_no:
            if form_type in CONSTITUENCY_FORMS | PARTYLIST_FORMS:
                validated.at[index, "validation_status"] = "needs_review"
            continue
        if form_type in CONSTITUENCY_FORMS and candidate_keys:
            if _candidate_result_key(row, config) not in candidate_keys:
                validated.at[index, "validation_status"] = "needs_review"
        elif form_type in PARTYLIST_FORMS and party_keys:
            if choice_no not in party_keys:
                validated.at[index, "validation_status"] = "needs_review"
    return validated


def _first_filled(series: pd.Series) -> object:
    for value in series:
        if pd.notna(value) and str(value).strip() != "":
            return value
    return pd.NA


def _best_numeric(series: pd.Series) -> object:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return pd.NA
    return numeric.iloc[0]


def deduplicate_result_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    key_columns = ["form_type", "polling_station_no", "choice_no"]
    if not set(key_columns).issubset(df.columns):
        return df

    keyed = df.dropna(subset=["choice_no"]).copy()
    unkeyed = df[df["choice_no"].isna()].copy()
    if keyed.empty:
        return df

    keyed["_has_votes"] = pd.to_numeric(keyed["votes"], errors="coerce").notna()
    keyed["_is_ok"] = keyed["validation_status"].astype(str).eq("ok")
    keyed["_ocr_confidence_sort"] = pd.to_numeric(
        keyed["ocr_confidence"], errors="coerce"
    ).fillna(0.0)
    keyed = keyed.sort_values(
        key_columns + ["_is_ok", "_has_votes", "_ocr_confidence_sort"],
        ascending=[True, True, True, False, False, False],
    )

    merged_rows: list[dict[str, object]] = []
    for _, group in keyed.groupby(key_columns, dropna=False, sort=False):
        best = group.iloc[0].copy()
        for column in NUMERIC_COLUMNS:
            if column in {"votes", "choice_no", "polling_station_no", "constituency_no", "source_page"}:
                continue
            best[column] = _best_numeric(group[column])
        best["votes"] = _best_numeric(group["votes"])
        for column in RESULT_COLUMNS:
            if column in NUMERIC_COLUMNS or column not in group.columns:
                continue
            best[column] = _first_filled(group[column])
        if len(group) > 1:
            distinct_votes = pd.to_numeric(group["votes"], errors="coerce").dropna().nunique()
            if distinct_votes > 1 or pd.isna(best["votes"]):
                best["validation_status"] = "needs_review"
        merged_rows.append({column: best.get(column, pd.NA) for column in RESULT_COLUMNS})

    merged = pd.DataFrame(merged_rows, columns=RESULT_COLUMNS)
    if not unkeyed.empty:
        merged = pd.concat([merged, unkeyed[RESULT_COLUMNS]], ignore_index=True)
    return merged[RESULT_COLUMNS]


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
    if "reviewed_rows_file" in config.paths:
        cleaned = apply_reviewed_rows(cleaned, config.path("reviewed_rows_file"))
        cleaned = normalize_results(cleaned)
    cleaned = apply_station_inference(cleaned, config)
    cleaned = apply_master_names(cleaned, config)
    cleaned = normalize_results(cleaned)
    cleaned = deduplicate_result_rows(cleaned)
    cleaned = apply_master_key_validation(cleaned, config)
    cleaned = normalize_results(cleaned)

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
