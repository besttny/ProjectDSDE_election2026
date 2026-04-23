from pathlib import Path

import pandas as pd

from src.pipeline.config import ProjectConfig
from src.quality.error_analysis import build_error_analysis


def _config(root: Path) -> ProjectConfig:
    return ProjectConfig(
        root=root,
        data={
            "outputs": {
                "review_queue": "data/processed/review_queue.csv",
                "validation_report": "data/processed/validation_report.csv",
                "accuracy_report": "data/processed/accuracy_report.csv",
            }
        },
    )


def test_build_error_analysis_groups_review_reasons_and_validation(tmp_path: Path):
    processed = tmp_path / "data/processed"
    processed.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "form_type": "5_18",
                "reason": "missing_votes",
                "priority": "P0",
                "source_pdf": "a.pdf",
                "source_page": 1,
            },
            {
                "form_type": "5_18",
                "reason": "missing_votes",
                "priority": "P0",
                "source_pdf": "a.pdf",
                "source_page": 1,
            },
            {
                "form_type": "5_18_partylist",
                "reason": "low_ocr_confidence",
                "priority": "P1",
                "source_pdf": "b.pdf",
                "source_page": 2,
            },
        ]
    ).to_csv(processed / "review_queue.csv", index=False)
    pd.DataFrame(
        [
            {"check": "needs_review_rows", "status": "warn", "severity": "major", "details": "3 rows"},
            {"check": "schema", "status": "pass", "severity": "info", "details": ""},
        ]
    ).to_csv(processed / "validation_report.csv", index=False)
    pd.DataFrame(
        [
            {
                "metric": "row_exact_accuracy",
                "status": "fail",
                "total": 10,
                "details": "below target",
            }
        ]
    ).to_csv(processed / "accuracy_report.csv", index=False)

    report = build_error_analysis(_config(tmp_path))

    grouped = report[
        (report["section"] == "review_reason_by_form")
        & (report["form_type"] == "5_18")
        & (report["reason"] == "missing_votes")
    ].iloc[0]
    assert grouped["count"] == 2
    assert "source_page_hotspots" in set(report["section"])
    assert "validation_non_pass" in set(report["section"])
    assert "accuracy_non_pass" in set(report["section"])
