from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.ocr.text_constraints import invalid_thai_text_mask
from src.pipeline.clean import clean_results
from src.pipeline.config import ProjectConfig, load_config
from src.pipeline.manifest import ManifestEntry, load_manifest, write_manifest_status
from src.pipeline.schema import ELECTION_DAY_FORMS, REQUIRED_FORMS, RESULT_COLUMNS

CANDIDATE_MASTER_SCOPE_COLUMNS = ["province", "constituency_no", "candidate_no"]
CANDIDATE_MASTER_REQUIRED_COLUMNS = [
    *CANDIDATE_MASTER_SCOPE_COLUMNS,
    "canonical_name",
    "party_name",
]


def _row(check: str, status: str, severity: str, details: str) -> dict[str, str]:
    return {
        "check": check,
        "status": status,
        "severity": severity,
        "details": details,
    }


def _read_results(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=RESULT_COLUMNS)
    return pd.read_csv(path)


def _normalize_text_key(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).replace("\ufeff", "").strip()


def _normalize_number_key(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.notna(numeric) and float(numeric).is_integer():
        return str(int(numeric))
    return _normalize_text_key(value)


def _candidate_master_checks(candidate_master_path: Path | None) -> list[dict[str, str]]:
    if candidate_master_path is None:
        return []
    if not candidate_master_path.exists():
        return [
            _row(
                "candidate_master_schema",
                "fail",
                "critical",
                f"Candidate master file not found: {candidate_master_path}",
            )
        ]

    frame = pd.read_csv(candidate_master_path).fillna("")
    missing_columns = [
        column for column in CANDIDATE_MASTER_REQUIRED_COLUMNS if column not in frame.columns
    ]
    checks = [
        _row(
            "candidate_master_schema",
            "fail" if missing_columns else "pass",
            "critical",
            (
                f"Missing columns: {', '.join(missing_columns)}"
                if missing_columns
                else "Candidate master is scoped by province + constituency_no + candidate_no"
            ),
        )
    ]
    if missing_columns:
        return checks

    scoped = frame.copy()
    scoped["_province_key"] = scoped["province"].map(_normalize_text_key)
    scoped["_constituency_key"] = scoped["constituency_no"].map(_normalize_number_key)
    scoped["_candidate_key"] = scoped["candidate_no"].map(_normalize_number_key)
    scoped["_candidate_name_key"] = scoped["canonical_name"].map(_normalize_text_key)
    scoped["_district_key"] = scoped["_province_key"] + "|" + scoped["_constituency_key"]

    missing_key_rows = int(
        (
            (scoped["_province_key"] == "")
            | (scoped["_constituency_key"] == "")
            | (scoped["_candidate_key"] == "")
        ).sum()
    )
    duplicate_key_rows = int(
        scoped.duplicated(
            subset=["_province_key", "_constituency_key", "_candidate_key"],
            keep=False,
        ).sum()
    )
    checks.append(
        _row(
            "candidate_master_scoped_keys",
            "fail" if missing_key_rows or duplicate_key_rows else "pass",
            "critical",
            (
                f"{missing_key_rows} rows have incomplete scope; "
                f"{duplicate_key_rows} rows duplicate province + constituency_no + candidate_no"
            ),
        )
    )

    candidate_names = scoped[scoped["_candidate_name_key"] != ""]
    repeated_people = 0
    if not candidate_names.empty:
        person_scope_counts = candidate_names.groupby("_candidate_name_key")["_district_key"].nunique()
        repeated_people = int((person_scope_counts > 1).sum())
    checks.append(
        _row(
            "candidate_master_one_person_one_constituency",
            "fail" if repeated_people else "pass",
            "critical",
            f"{repeated_people} candidate names appear in more than one province + constituency_no",
        )
    )

    unscoped_reuse = 0
    if not scoped.empty:
        number_scope_counts = scoped.groupby("_candidate_key")["_district_key"].nunique()
        unscoped_reuse = int((number_scope_counts > 1).sum())
    checks.append(
        _row(
            "candidate_master_candidate_no_scoped",
            "pass",
            "major",
            (
                f"{unscoped_reuse} candidate numbers are reused across constituencies; "
                "mapping uses province + constituency_no + candidate_no, not candidate_no alone"
            ),
        )
    )
    return checks


def _candidate_master_key_set(candidate_master_path: Path | None) -> set[tuple[str, str, str]]:
    if candidate_master_path is None or not candidate_master_path.exists():
        return set()
    frame = pd.read_csv(candidate_master_path).fillna("")
    if not set(CANDIDATE_MASTER_SCOPE_COLUMNS).issubset(frame.columns):
        return set()
    return {
        (
            _normalize_text_key(row["province"]),
            _normalize_number_key(row["constituency_no"]),
            _normalize_number_key(row["candidate_no"]),
        )
        for _, row in frame.iterrows()
        if _normalize_text_key(row["province"])
        and _normalize_number_key(row["constituency_no"])
        and _normalize_number_key(row["candidate_no"])
    }


def _party_master_key_set(party_master_path: Path | None) -> set[str]:
    if party_master_path is None or not party_master_path.exists():
        return set()
    frame = pd.read_csv(party_master_path).fillna("")
    if "party_no" not in frame.columns:
        return set()
    return {
        _normalize_number_key(row["party_no"])
        for _, row in frame.iterrows()
        if _normalize_number_key(row["party_no"])
    }


def _result_master_match_checks(
    df: pd.DataFrame,
    *,
    candidate_master_path: Path | None,
    party_master_path: Path | None,
) -> list[dict[str, str]]:
    if df.empty:
        return [
            _row("result_candidate_master_matches", "warn", "major", "No rows to compare"),
            _row("result_party_master_matches", "warn", "major", "No rows to compare"),
        ]

    candidate_keys = _candidate_master_key_set(candidate_master_path)
    party_keys = _party_master_key_set(party_master_path)
    report: list[dict[str, str]] = []

    forms = df.get("form_type", pd.Series(dtype=str)).astype(str)
    statuses = df.get("validation_status", pd.Series(dtype=str)).astype(str)

    candidate_rows = df[forms.isin({"5_16", "5_17", "5_18"})]
    if candidate_keys:
        unmatched_indexes: list[int] = []
        for index, row in candidate_rows.iterrows():
            key = (
                _normalize_text_key(row.get("province", "")),
                _normalize_number_key(row.get("constituency_no", "")),
                _normalize_number_key(row.get("choice_no", "")),
            )
            if "" in key or key not in candidate_keys:
                unmatched_indexes.append(index)
        ok_unmatched = int(statuses.loc[unmatched_indexes].eq("ok").sum()) if unmatched_indexes else 0
        report.append(
            _row(
                "result_candidate_master_matches",
                "fail" if ok_unmatched else ("warn" if unmatched_indexes else "pass"),
                "major",
                (
                    f"{len(unmatched_indexes)} constituency result rows are not in candidate master; "
                    f"{ok_unmatched} are still marked ok"
                ),
            )
        )
    else:
        report.append(
            _row(
                "result_candidate_master_matches",
                "warn",
                "major",
                "No candidate master keys available for result comparison",
            )
        )

    party_rows = df[forms.isin({"5_16_partylist", "5_17_partylist", "5_18_partylist"})]
    if party_keys:
        unmatched_indexes = []
        for index, row in party_rows.iterrows():
            choice_no = _normalize_number_key(row.get("choice_no", ""))
            if not choice_no or choice_no not in party_keys:
                unmatched_indexes.append(index)
        ok_unmatched = int(statuses.loc[unmatched_indexes].eq("ok").sum()) if unmatched_indexes else 0
        report.append(
            _row(
                "result_party_master_matches",
                "fail" if ok_unmatched else ("warn" if unmatched_indexes else "pass"),
                "major",
                (
                    f"{len(unmatched_indexes)} party-list result rows are not in party master; "
                    f"{ok_unmatched} are still marked ok"
                ),
            )
        )
    else:
        report.append(
            _row(
                "result_party_master_matches",
                "warn",
                "major",
                "No party master keys available for result comparison",
            )
        )
    return report


def validate_dataframe(
    df: pd.DataFrame,
    *,
    manifest_entries: list[ManifestEntry],
    expected_polling_stations: int,
    candidate_master_path: Path | None = None,
    party_master_path: Path | None = None,
) -> pd.DataFrame:
    report: list[dict[str, str]] = []

    missing_files = [entry for entry in manifest_entries if entry.required and not entry.exists]
    report.append(
        _row(
            "required_pdf_files",
            "fail" if missing_files else "pass",
            "critical",
            f"{len(missing_files)} required PDFs missing"
            if missing_files
            else "All required manifest PDFs are present",
        )
    )

    report.append(
        _row(
            "parsed_rows",
            "fail" if df.empty else "pass",
            "critical",
            f"{len(df):,} parsed result rows",
        )
    )

    present_forms = set(df["form_type"].dropna().astype(str)) if "form_type" in df else set()
    missing_forms = [form for form in REQUIRED_FORMS if form not in present_forms]
    report.append(
        _row(
            "required_forms_present",
            "fail" if missing_forms else "pass",
            "critical",
            f"Missing forms: {', '.join(missing_forms)}" if missing_forms else "All required forms found",
        )
    )
    report.extend(_candidate_master_checks(candidate_master_path))
    report.extend(
        _result_master_match_checks(
            df,
            candidate_master_path=candidate_master_path,
            party_master_path=party_master_path,
        )
    )

    invalid_text = invalid_thai_text_mask(df)
    invalid_text_count = int(invalid_text.sum())
    invalid_text_ok = int(
        (
            invalid_text
            & df.get("validation_status", pd.Series(dtype=str)).astype(str).eq("ok")
        ).sum()
    )
    report.append(
        _row(
            "thai_text_charset",
            "fail" if invalid_text_ok else ("warn" if invalid_text_count else "pass"),
            "major",
            (
                f"{invalid_text_count} rows contain non-Thai text in Thai fields; "
                f"{invalid_text_ok} are still marked ok"
            ),
        )
    )

    for form in ELECTION_DAY_FORMS:
        form_df = df[df.get("form_type", pd.Series(dtype=str)).astype(str) == form]
        stations = (
            form_df["polling_station_no"].dropna().nunique()
            if not form_df.empty and "polling_station_no" in form_df.columns
            else 0
        )
        report.append(
            _row(
                f"{form}_station_coverage",
                "pass" if stations == expected_polling_stations else "fail",
                "critical",
                f"{stations} / {expected_polling_stations} polling stations parsed",
            )
        )

    negative_votes = int((pd.to_numeric(df.get("votes", pd.Series(dtype=float)), errors="coerce") < 0).sum())
    report.append(
        _row(
            "non_negative_votes",
            "fail" if negative_votes else "pass",
            "critical",
            f"{negative_votes} rows have negative votes",
        )
    )

    duplicate_count = 0
    duplicate_subset = ["form_type", "polling_station_no", "choice_no"]
    if not df.empty and set(duplicate_subset).issubset(df.columns):
        duplicate_base = df.dropna(subset=["polling_station_no", "choice_no"])
        duplicate_count = int(
            duplicate_base.duplicated(subset=duplicate_subset, keep=False).sum()
        )
    report.append(
        _row(
            "duplicate_choice_rows",
            "fail" if duplicate_count else "pass",
            "critical",
            f"{duplicate_count} duplicate rows by form + station + choice; rows without station number are excluded",
        )
    )

    exceeded = 0
    if not df.empty and {"form_type", "polling_station_no", "votes", "valid_votes"}.issubset(df.columns):
        grouped = (
            df.dropna(subset=["valid_votes"])
            .groupby(["form_type", "polling_station_no"], dropna=False)
            .agg(total_votes=("votes", "sum"), valid_votes=("valid_votes", "max"))
            .reset_index()
        )
        exceeded = int((grouped["total_votes"] > grouped["valid_votes"]).sum())
        totals_status = "fail" if exceeded else ("pass" if not grouped.empty else "warn")
        totals_detail = (
            f"{exceeded} station/form groups exceed valid_votes"
            if not grouped.empty
            else "No extracted valid_votes available for total comparison"
        )
    else:
        totals_status = "warn"
        totals_detail = "Required columns unavailable for total comparison"
    report.append(_row("choice_votes_not_over_valid_votes", totals_status, "major", totals_detail))

    accounting_mismatches = 0
    accounting_available = 0
    accounting_columns = {"ballots_cast", "valid_votes", "invalid_votes", "no_vote"}
    if not df.empty and accounting_columns.issubset(df.columns):
        accounting = df[list(accounting_columns)].apply(pd.to_numeric, errors="coerce")
        complete = accounting.notna().all(axis=1)
        accounting_available = int(complete.sum())
        expected_total = (
            accounting["valid_votes"] + accounting["invalid_votes"] + accounting["no_vote"]
        )
        accounting_mismatches = int((complete & (expected_total != accounting["ballots_cast"])).sum())
        accounting_status = (
            "fail"
            if accounting_mismatches
            else ("pass" if accounting_available else "warn")
        )
        accounting_detail = (
            f"{accounting_mismatches} rows have valid + invalid + no_vote != ballots_cast"
            if accounting_available
            else "No complete ballot accounting fields available"
        )
    else:
        accounting_status = "warn"
        accounting_detail = "Required columns unavailable for ballot accounting"
    report.append(_row("ballot_accounting", accounting_status, "major", accounting_detail))

    needs_review = (
        int((df.get("validation_status", pd.Series(dtype=str)).astype(str) != "ok").sum())
        if not df.empty
        else 0
    )
    report.append(
        _row(
            "needs_review_rows",
            "warn" if needs_review else "pass",
            "major",
            f"{needs_review} rows require manual review",
        )
    )

    return pd.DataFrame(report)


def write_markdown_report(report: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Validation Report", "", "| Check | Status | Severity | Details |", "|---|---|---|---|"]
    for _, row in report.iterrows():
        lines.append(
            f"| {row['check']} | {row['status']} | {row['severity']} | {row['details']} |"
        )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def validate_results(config: ProjectConfig) -> tuple[Path, Path]:
    config.ensure_output_dirs()
    entries = load_manifest(config)
    write_manifest_status(config, entries)
    results_path = config.output("election_results")
    if not results_path.exists():
        clean_results(config)
    df = _read_results(results_path)
    report = validate_dataframe(
        df,
        manifest_entries=entries,
        expected_polling_stations=config.expected_polling_stations,
        candidate_master_path=(
            config.path("master_candidates_file")
            if "master_candidates_file" in config.paths
            else None
        ),
        party_master_path=(
            config.path("master_parties_file")
            if "master_parties_file" in config.paths
            else None
        ),
    )
    csv_path = config.output("validation_report")
    report.to_csv(csv_path, index=False, encoding="utf-8-sig")
    md_path = config.output("validation_report_md")
    write_markdown_report(report, md_path)
    return csv_path, md_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate cleaned election results.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    args = parser.parse_args()
    csv_path, md_path = validate_results(load_config(args.config))
    print(csv_path)
    print(md_path)


if __name__ == "__main__":
    main()
