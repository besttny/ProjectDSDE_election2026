from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.pipeline.config import ProjectConfig, load_config
from src.quality.review_queue import write_review_queue

ERROR_ANALYSIS_COLUMNS = [
    "section",
    "form_type",
    "reason",
    "priority",
    "count",
    "details",
]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path).fillna("")


def _review_queue(config: ProjectConfig) -> pd.DataFrame:
    path = config.output("review_queue")
    if not path.exists():
        write_review_queue(config)
    return _read_csv(path)


def _append_review_reason_rows(rows: list[dict[str, object]], queue: pd.DataFrame) -> None:
    required = {"form_type", "reason", "priority"}
    if queue.empty or not required.issubset(queue.columns):
        return
    grouped = (
        queue.groupby(["form_type", "reason", "priority"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["count", "form_type", "reason"], ascending=[False, True, True])
    )
    for _, record in grouped.iterrows():
        rows.append(
            {
                "section": "review_reason_by_form",
                "form_type": record["form_type"],
                "reason": record["reason"],
                "priority": record["priority"],
                "count": int(record["count"]),
                "details": "Review queue rows grouped by OCR/validation failure reason.",
            }
        )


def _append_hotspot_rows(rows: list[dict[str, object]], queue: pd.DataFrame) -> None:
    required = {"source_pdf", "source_page", "form_type", "reason", "priority"}
    if queue.empty or not required.issubset(queue.columns):
        return
    grouped = (
        queue.groupby(["source_pdf", "source_page", "form_type"], dropna=False)
        .agg(count=("reason", "size"), reasons=("reason", lambda values: ", ".join(sorted(set(map(str, values))))))
        .reset_index()
        .sort_values(["count", "source_pdf", "source_page"], ascending=[False, True, True])
        .head(25)
    )
    for _, record in grouped.iterrows():
        source_page = str(record["source_page"]).strip()
        rows.append(
            {
                "section": "source_page_hotspots",
                "form_type": record["form_type"],
                "reason": "multiple",
                "priority": "",
                "count": int(record["count"]),
                "details": (
                    f"{record['source_pdf']} page {source_page}: {record['reasons']}"
                ),
            }
        )


def _append_validation_rows(rows: list[dict[str, object]], config: ProjectConfig) -> None:
    path = config.output("validation_report")
    validation = _read_csv(path)
    if validation.empty or "status" not in validation.columns:
        return
    for _, record in validation[validation["status"].astype(str).ne("pass")].iterrows():
        rows.append(
            {
                "section": "validation_non_pass",
                "form_type": "",
                "reason": record.get("check", ""),
                "priority": record.get("severity", ""),
                "count": "",
                "details": record.get("details", ""),
            }
        )


def _append_accuracy_rows(rows: list[dict[str, object]], config: ProjectConfig) -> None:
    path = config.output("accuracy_report")
    accuracy = _read_csv(path)
    if accuracy.empty or "status" not in accuracy.columns:
        return
    for _, record in accuracy[accuracy["status"].astype(str).ne("pass")].iterrows():
        rows.append(
            {
                "section": "accuracy_non_pass",
                "form_type": "",
                "reason": record.get("metric", ""),
                "priority": "target",
                "count": record.get("total", ""),
                "details": record.get("details", ""),
            }
        )


def build_error_analysis(config: ProjectConfig) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    queue = _review_queue(config)
    _append_review_reason_rows(rows, queue)
    _append_hotspot_rows(rows, queue)
    _append_validation_rows(rows, config)
    _append_accuracy_rows(rows, config)
    return pd.DataFrame(rows, columns=ERROR_ANALYSIS_COLUMNS)


def write_error_analysis(config: ProjectConfig) -> tuple[Path, Path]:
    config.ensure_output_dirs()
    report = build_error_analysis(config)
    csv_path = config.output("error_analysis_report")
    md_path = config.output("error_analysis_report_md")
    report.to_csv(csv_path, index=False, encoding="utf-8-sig")
    _write_markdown(report, md_path)
    return csv_path, md_path


def _write_markdown(report: pd.DataFrame, output_path: Path) -> None:
    lines = [
        "# OCR Error Analysis",
        "",
        "| Section | Form | Reason | Priority | Count | Details |",
        "|---|---|---|---|---:|---|",
    ]
    for _, row in report.iterrows():
        lines.append(
            "| {section} | {form_type} | {reason} | {priority} | {count} | {details} |".format(
                section=row.get("section", ""),
                form_type=row.get("form_type", ""),
                reason=row.get("reason", ""),
                priority=row.get("priority", ""),
                count=row.get("count", ""),
                details=str(row.get("details", "")).replace("|", "\\|"),
            )
        )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build OCR error analysis report.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    args = parser.parse_args()
    csv_path, md_path = write_error_analysis(load_config(args.config))
    print(csv_path)
    print(md_path)


if __name__ == "__main__":
    main()
