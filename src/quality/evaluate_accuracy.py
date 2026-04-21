from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.pipeline.config import ProjectConfig, load_config
from src.pipeline.final_export import FORM_MAP, export_final_schema


GROUND_TRUTH_COLUMNS = [
    "ballot_type",
    "form_type",
    "station_id",
    "choice_no",
    "votes",
    "total_ballots",
    "invalid_ballots",
    "no_vote_ballots",
    "name",
    "party",
    "source_page",
    "reviewer_notes",
]

KEY_COLUMNS = ["ballot_type", "form_type", "station_id", "choice_no"]
FIELD_COLUMNS = ["votes", "total_ballots", "invalid_ballots", "no_vote_ballots", "name", "party"]
NUMERIC_FIELDS = {"votes", "total_ballots", "invalid_ballots", "no_vote_ballots"}
REPORT_COLUMNS = ["metric", "status", "correct", "total", "accuracy", "target", "details"]
DETAIL_COLUMNS = KEY_COLUMNS + ["field", "expected_value", "predicted_value", "match", "status"]


def _read_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(path).fillna("")
    if not columns:
        return frame
    for column in columns:
        if column not in frame.columns:
            frame[column] = ""
    return frame[columns]


def _normalize_id(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.notna(numeric) and float(numeric).is_integer():
        return str(int(numeric))
    return text


def _normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).split()).strip()


def _normalize_form_type(value: object) -> str:
    text = _normalize_text(value)
    if text in FORM_MAP:
        return FORM_MAP[text][0]
    compact = text.replace("/", "_")
    if compact in FORM_MAP:
        return FORM_MAP[compact][0]
    return text


def _normalize_ballot_type(value: object) -> str:
    text = _normalize_text(value).casefold()
    aliases = {
        "constituency_votes": "constituency",
        "constituency": "constituency",
        "candidate": "constituency",
        "แบ่งเขต": "constituency",
        "partylist_votes": "partylist",
        "party_list": "partylist",
        "partylist": "partylist",
        "บัญชีรายชื่อ": "partylist",
    }
    return aliases.get(text, text)


def _normalize_compare(value: object, field: str) -> str:
    if field in NUMERIC_FIELDS:
        return _normalize_id(value)
    return _normalize_text(value)


def load_ground_truth(config: ProjectConfig) -> pd.DataFrame:
    frame = _read_csv(config.path("ground_truth_file"), GROUND_TRUTH_COLUMNS)
    if frame.empty:
        return frame
    normalized = frame.copy()
    normalized["ballot_type"] = normalized["ballot_type"].map(_normalize_ballot_type)
    normalized["form_type"] = normalized["form_type"].map(_normalize_form_type)
    normalized["station_id"] = normalized["station_id"].map(_normalize_id)
    normalized["choice_no"] = normalized["choice_no"].map(_normalize_id)
    for field in FIELD_COLUMNS:
        normalized[field] = normalized[field].map(lambda value, field=field: _normalize_compare(value, field))
    return normalized


def load_predictions(config: ProjectConfig) -> pd.DataFrame:
    constituency_path, partylist_path = export_final_schema(config)
    constituency = _read_csv(constituency_path, [])
    partylist = _read_csv(partylist_path, [])

    frames: list[pd.DataFrame] = []
    if not constituency.empty:
        frame = pd.DataFrame(index=constituency.index)
        frame["ballot_type"] = "constituency"
        frame["form_type"] = constituency.get("form_type", "")
        frame["station_id"] = constituency.get("station_id", "")
        frame["choice_no"] = constituency.get("candidate_no", "")
        frame["votes"] = constituency.get("votes", "")
        frame["total_ballots"] = constituency.get("total_ballots", "")
        frame["invalid_ballots"] = constituency.get("invalid_ballots", "")
        frame["no_vote_ballots"] = constituency.get("no_vote_ballots", "")
        frame["name"] = constituency.get("candidate_name", "")
        frame["party"] = constituency.get("party", "")
        frames.append(frame)
    if not partylist.empty:
        frame = pd.DataFrame(index=partylist.index)
        frame["ballot_type"] = "partylist"
        frame["form_type"] = partylist.get("form_type", "")
        frame["station_id"] = partylist.get("station_id", "")
        frame["choice_no"] = partylist.get("party_no", "")
        frame["votes"] = partylist.get("votes", "")
        frame["total_ballots"] = partylist.get("total_ballots", "")
        frame["invalid_ballots"] = partylist.get("invalid_ballots", "")
        frame["no_vote_ballots"] = partylist.get("no_vote_ballots", "")
        frame["name"] = ""
        frame["party"] = partylist.get("party", "")
        frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=KEY_COLUMNS + FIELD_COLUMNS)

    predictions = pd.concat(frames, ignore_index=True)
    predictions["form_type"] = predictions["form_type"].map(_normalize_form_type)
    predictions["station_id"] = predictions["station_id"].map(_normalize_id)
    predictions["choice_no"] = predictions["choice_no"].map(_normalize_id)
    for field in FIELD_COLUMNS:
        predictions[field] = predictions[field].map(lambda value, field=field: _normalize_compare(value, field))
    return predictions[KEY_COLUMNS + FIELD_COLUMNS]


def _report_row(metric: str, correct: int, total: int, target: float, details: str) -> dict[str, object]:
    if total == 0:
        accuracy = ""
        status = "warn"
    else:
        accuracy_value = correct / total
        accuracy = round(accuracy_value, 4)
        status = "pass" if accuracy_value >= target else "fail"
    return {
        "metric": metric,
        "status": status,
        "correct": correct,
        "total": total,
        "accuracy": accuracy,
        "target": target,
        "details": details,
    }


def evaluate_accuracy(config: ProjectConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    target = float(config.quality.get("target_accuracy", 0.99))
    ground_truth = load_ground_truth(config)
    if ground_truth.empty:
        report = pd.DataFrame(
            [
                _report_row(
                    "ground_truth_rows",
                    0,
                    0,
                    target,
                    f"No rows found in {config.path('ground_truth_file')}",
                ),
                _report_row(
                    "overall_field_accuracy",
                    0,
                    0,
                    target,
                    "Add reviewed rows before claiming final accuracy.",
                ),
            ],
            columns=REPORT_COLUMNS,
        )
        return report, pd.DataFrame(columns=DETAIL_COLUMNS)

    predictions = load_predictions(config)
    merged = ground_truth.merge(
        predictions,
        on=KEY_COLUMNS,
        how="left",
        suffixes=("_expected", "_predicted"),
        indicator=True,
    )

    details: list[dict[str, object]] = []
    for _, row in merged.iterrows():
        missing_prediction = row["_merge"] == "left_only"
        for field in FIELD_COLUMNS:
            expected = row.get(f"{field}_expected", "")
            if not expected:
                continue
            predicted = "" if missing_prediction else row.get(f"{field}_predicted", "")
            matched = expected == predicted
            details.append(
                {
                    "ballot_type": row["ballot_type"],
                    "form_type": row["form_type"],
                    "station_id": row["station_id"],
                    "choice_no": row["choice_no"],
                    "field": field,
                    "expected_value": expected,
                    "predicted_value": predicted,
                    "match": matched,
                    "status": "match" if matched else ("missing_prediction" if missing_prediction else "mismatch"),
                }
            )

    details_df = pd.DataFrame(details, columns=DETAIL_COLUMNS)
    if details_df.empty:
        report = pd.DataFrame(
            [
                _report_row(
                    "overall_field_accuracy",
                    0,
                    0,
                    target,
                    "Ground truth has rows but no comparable expected fields.",
                )
            ],
            columns=REPORT_COLUMNS,
        )
        return report, details_df

    report_rows = [
        _report_row(
            "ground_truth_rows",
            len(ground_truth),
            len(ground_truth),
            target,
            "Reviewed sample rows loaded.",
        )
    ]
    overall_correct = int(details_df["match"].sum())
    overall_total = len(details_df)
    report_rows.append(
        _report_row(
            "overall_field_accuracy",
            overall_correct,
            overall_total,
            target,
            "All filled ground-truth fields compared against final CSV outputs.",
        )
    )

    row_exact = (
        details_df.groupby(KEY_COLUMNS, dropna=False)["match"]
        .agg(lambda values: bool(values.all()))
        .reset_index(name="row_match")
    )
    report_rows.append(
        _report_row(
            "row_exact_accuracy",
            int(row_exact["row_match"].sum()),
            len(row_exact),
            target,
            "A row passes only when every filled ground-truth field matches.",
        )
    )

    for field in FIELD_COLUMNS:
        field_df = details_df[details_df["field"] == field]
        report_rows.append(
            _report_row(
                f"{field}_accuracy",
                int(field_df["match"].sum()),
                len(field_df),
                target,
                f"{field} compared where present in ground truth.",
            )
        )
    return pd.DataFrame(report_rows, columns=REPORT_COLUMNS), details_df


def write_markdown_report(report: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Accuracy Report", "", "| Metric | Status | Correct | Total | Accuracy | Target | Details |", "|---|---|---:|---:|---:|---:|---|"]
    for _, row in report.iterrows():
        lines.append(
            f"| {row['metric']} | {row['status']} | {row['correct']} | {row['total']} | {row['accuracy']} | {row['target']} | {row['details']} |"
        )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_accuracy_outputs(config: ProjectConfig) -> tuple[Path, Path, Path]:
    config.ensure_output_dirs()
    report, details = evaluate_accuracy(config)
    report_path = config.output("accuracy_report")
    details_path = config.output("accuracy_details")
    markdown_path = config.output("accuracy_report_md")
    report.to_csv(report_path, index=False, encoding="utf-8-sig")
    details.to_csv(details_path, index=False, encoding="utf-8-sig")
    write_markdown_report(report, markdown_path)
    return report_path, details_path, markdown_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate final CSV accuracy against reviewed ground truth.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    args = parser.parse_args()
    for path in write_accuracy_outputs(load_config(args.config)):
        print(path)


if __name__ == "__main__":
    main()
