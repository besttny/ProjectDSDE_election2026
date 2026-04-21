from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.pipeline.clean import clean_results
from src.pipeline.config import ProjectConfig, load_config
from src.pipeline.manifest import ManifestEntry, load_manifest, write_manifest_status
from src.pipeline.schema import ELECTION_DAY_FORMS, REQUIRED_FORMS, RESULT_COLUMNS


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


def validate_dataframe(
    df: pd.DataFrame,
    *,
    manifest_entries: list[ManifestEntry],
    expected_polling_stations: int,
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

    for form in ELECTION_DAY_FORMS:
        form_df = df[df.get("form_type", pd.Series(dtype=str)).astype(str) == form]
        stations = form_df["polling_station_no"].dropna().nunique() if not form_df.empty else 0
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
        duplicate_count = int(df.duplicated(subset=duplicate_subset, keep=False).sum())
    report.append(
        _row(
            "duplicate_choice_rows",
            "fail" if duplicate_count else "pass",
            "critical",
            f"{duplicate_count} duplicate rows by form + station + choice",
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

