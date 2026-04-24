from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.clean import clean_results
from src.pipeline.config import ProjectConfig, load_config
from src.pipeline.schema import RESULT_COLUMNS

CONSTITUENCY_FORMS = {"5_16", "5_17", "5_18"}
PARTYLIST_FORMS = {"5_16_partylist", "5_17_partylist", "5_18_partylist"}
SUMMARY_FIELD_MAP = {
    "eligible_voters": "eligible_voters",
    "ballots_cast": "ballots_cast",
    "total_ballots": "ballots_cast",
    "valid_votes": "valid_votes",
    "invalid_votes": "invalid_votes",
    "invalid_ballots": "invalid_votes",
    "no_vote": "no_vote",
    "no_vote_ballots": "no_vote",
}
REFERENCE_COLUMNS = [
    "ballot_type",
    "field",
    "choice_no",
    "expected_value",
    "source_form",
    "source_url",
    "notes",
]
REPORT_COLUMNS = [
    "ballot_type",
    "field",
    "choice_no",
    "expected_value",
    "actual_value",
    "difference",
    "status",
    "source_form",
    "source_url",
    "notes",
]


def _normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).replace("\ufeff", "").strip()


def _normalize_number_key(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.notna(numeric) and float(numeric).is_integer():
        return str(int(numeric))
    return _normalize_text(value)


def _normalize_ballot_type(value: object) -> str:
    text = _normalize_text(value).casefold()
    compact = text.replace("_", "").replace("-", "").replace(" ", "")
    if compact in {"partylist", "party", "bch"} or "บัญชีรายชื่อ" in text or "บช" in text:
        return "party_list"
    if compact in {"constituency", "candidate", "district"} or "แบ่งเขต" in text or "เขต" in text:
        return "constituency"
    return text


def _normalize_field(value: object, *, choice_no: object = "") -> str:
    text = _normalize_text(value).casefold()
    if not text:
        return "votes" if _normalize_number_key(choice_no) else ""
    return SUMMARY_FIELD_MAP.get(text, text)


def _numeric_sum(values: pd.Series) -> int | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return None
    return int(numeric.sum())


def _reference_path(config: ProjectConfig) -> Path:
    if "aggregate_validation_reference_file" in config.paths:
        return config.path("aggregate_validation_reference_file")
    return config.path("external_dir") / "aggregate_validation_reference.csv"


def _read_results(config: ProjectConfig) -> pd.DataFrame:
    path = config.output("election_results")
    if not path.exists():
        clean_results(config)
    if not path.exists():
        return pd.DataFrame(columns=RESULT_COLUMNS)
    return pd.read_csv(path).fillna("")


def _read_reference(config: ProjectConfig) -> pd.DataFrame:
    path = _reference_path(config)
    if not path.exists():
        return pd.DataFrame(columns=REFERENCE_COLUMNS)
    reference = pd.read_csv(path).fillna("")
    if "expected_value" not in reference.columns:
        for column in ["expected_total", "votes", "value"]:
            if column in reference.columns:
                reference["expected_value"] = reference[column]
                break
    if "field" not in reference.columns:
        reference["field"] = ""
    for column in REFERENCE_COLUMNS:
        if column not in reference.columns:
            reference[column] = ""
    reference["ballot_type"] = reference["ballot_type"].map(_normalize_ballot_type)
    reference["choice_no"] = reference["choice_no"].map(_normalize_number_key)
    reference["field"] = [
        _normalize_field(field, choice_no=choice_no)
        for field, choice_no in zip(reference["field"], reference["choice_no"], strict=False)
    ]
    return reference[REFERENCE_COLUMNS]


def _choice_vote_actuals(df: pd.DataFrame) -> dict[tuple[str, str, str], int]:
    actuals: dict[tuple[str, str, str], int] = {}
    if df.empty or not {"form_type", "choice_no", "votes"}.issubset(df.columns):
        return actuals

    for ballot_type, forms in [
        ("constituency", CONSTITUENCY_FORMS),
        ("party_list", PARTYLIST_FORMS),
    ]:
        rows = df[df["form_type"].astype(str).isin(forms)].copy()
        if rows.empty:
            continue
        rows["_choice_no"] = rows["choice_no"].map(_normalize_number_key)
        rows["_votes"] = pd.to_numeric(rows["votes"], errors="coerce")
        rows = rows[rows["_choice_no"].astype(str).ne("") & rows["_votes"].notna()]
        grouped = rows.groupby("_choice_no", dropna=False)["_votes"].sum()
        for choice_no, total in grouped.items():
            actuals[(ballot_type, "votes", str(choice_no))] = int(total)
    return actuals


def _summary_actuals(df: pd.DataFrame) -> dict[tuple[str, str, str], int]:
    actuals: dict[tuple[str, str, str], int] = {}
    if df.empty or "form_type" not in df.columns:
        return actuals

    fields = sorted(set(SUMMARY_FIELD_MAP.values()))
    for ballot_type, forms in [
        ("constituency", CONSTITUENCY_FORMS),
        ("party_list", PARTYLIST_FORMS),
    ]:
        rows = df[df["form_type"].astype(str).isin(forms)].copy()
        if rows.empty:
            continue
        for field in fields:
            if field not in rows.columns:
                continue
            rows[field] = pd.to_numeric(rows[field], errors="coerce")
        key_columns = [
            column
            for column in ["form_type", "polling_station_no", "source_pdf", "source_page"]
            if column in rows.columns
        ]
        if not key_columns:
            key_columns = ["form_type"]
        station_summary = rows.groupby(key_columns, dropna=False)[
            [field for field in fields if field in rows.columns]
        ].max()
        for field in station_summary.columns:
            value = _numeric_sum(station_summary[field])
            if value is not None:
                actuals[(ballot_type, field, "")] = value
    return actuals


def _actuals(config: ProjectConfig) -> dict[tuple[str, str, str], int]:
    df = _read_results(config)
    actuals = _choice_vote_actuals(df)
    actuals.update(_summary_actuals(df))
    return actuals


def _as_int(value: object) -> int | None:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return int(numeric)


def build_aggregate_validation_report(config: ProjectConfig) -> pd.DataFrame:
    reference_path = _reference_path(config)
    reference = _read_reference(config)
    if reference.empty:
        return pd.DataFrame(
            [
                {
                    "ballot_type": "",
                    "field": "",
                    "choice_no": "",
                    "expected_value": "",
                    "actual_value": "",
                    "difference": "",
                    "status": "missing_reference",
                    "source_form": "",
                    "source_url": str(reference_path),
                    "notes": (
                        "Optional aggregate reference CSV not found. "
                        "Do not use 6/1 data to overwrite OCR rows; add it here only "
                        "for aggregate validation."
                    ),
                }
            ],
            columns=REPORT_COLUMNS,
        )

    actuals = _actuals(config)
    rows: list[dict[str, Any]] = []
    for _, record in reference.iterrows():
        ballot_type = _normalize_ballot_type(record.get("ballot_type", ""))
        field = _normalize_field(record.get("field", ""), choice_no=record.get("choice_no", ""))
        choice_no = _normalize_number_key(record.get("choice_no", ""))
        expected = _as_int(record.get("expected_value", ""))
        key = (ballot_type, field, choice_no if field == "votes" else "")
        actual = actuals.get(key)
        if ballot_type not in {"constituency", "party_list"} or not field or expected is None:
            status = "invalid_reference"
            difference: int | str = ""
        elif field == "votes" and not choice_no:
            status = "invalid_reference"
            difference = ""
        elif actual is None:
            status = "missing_actual"
            difference = ""
        else:
            difference = actual - expected
            status = "validated" if difference == 0 else "discrepancy"
        rows.append(
            {
                "ballot_type": ballot_type,
                "field": field,
                "choice_no": choice_no,
                "expected_value": "" if expected is None else expected,
                "actual_value": "" if actual is None else actual,
                "difference": difference,
                "status": status,
                "source_form": record.get("source_form", ""),
                "source_url": record.get("source_url", ""),
                "notes": record.get("notes", ""),
            }
        )
    return pd.DataFrame(rows, columns=REPORT_COLUMNS)


def write_aggregate_validation_report(config: ProjectConfig) -> tuple[Path, Path]:
    config.ensure_output_dirs()
    report = build_aggregate_validation_report(config)
    csv_path = config.output("aggregate_validation_report")
    md_path = config.output("aggregate_validation_report_md")
    report.to_csv(csv_path, index=False, encoding="utf-8-sig")
    _write_markdown(report, md_path)
    return csv_path, md_path


def _write_markdown(report: pd.DataFrame, output_path: Path) -> None:
    lines = [
        "# Aggregate Validation Report",
        "",
        "Aggregate references validate OCR sums only. They are never an overwrite source for row-level OCR data.",
        "",
        "| Ballot Type | Field | Choice | Expected | Actual | Difference | Status | Notes |",
        "|---|---|---:|---:|---:|---:|---|---|",
    ]
    for _, row in report.iterrows():
        lines.append(
            "| {ballot_type} | {field} | {choice_no} | {expected_value} | {actual_value} | {difference} | {status} | {notes} |".format(
                ballot_type=row.get("ballot_type", ""),
                field=row.get("field", ""),
                choice_no=row.get("choice_no", ""),
                expected_value=row.get("expected_value", ""),
                actual_value=row.get("actual_value", ""),
                difference=row.get("difference", ""),
                status=row.get("status", ""),
                notes=str(row.get("notes", "")).replace("|", "\\|"),
            )
        )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate OCR aggregates against optional 6/1 references.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    args = parser.parse_args()
    for path in write_aggregate_validation_report(load_config(args.config)):
        print(path)


if __name__ == "__main__":
    main()
