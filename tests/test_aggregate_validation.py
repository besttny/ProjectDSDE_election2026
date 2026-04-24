from pathlib import Path

import pandas as pd

from src.pipeline.config import ProjectConfig
from src.quality.aggregate_validation import (
    build_aggregate_validation_report,
    write_aggregate_validation_report,
)


def _config(root: Path) -> ProjectConfig:
    return ProjectConfig(
        root=root,
        data={
            "paths": {
                "raw_image_dir": "data/raw/images",
                "raw_ocr_dir": "data/raw/ocr",
                "parsed_dir": "data/processed/parsed",
                "processed_dir": "data/processed",
                "figures_dir": "outputs/figures",
                "reports_dir": "outputs/reports",
                "aggregate_validation_reference_file": "data/external/aggregate_validation_reference.csv",
            },
            "outputs": {
                "election_results": "data/processed/election_results_long.csv",
                "aggregate_validation_report": "data/processed/aggregate_validation_report.csv",
                "aggregate_validation_report_md": "outputs/reports/aggregate_validation_report.md",
            },
        },
    )


def test_aggregate_validation_compares_reference_without_overwriting_results(tmp_path: Path):
    processed = tmp_path / "data/processed"
    external = tmp_path / "data/external"
    processed.mkdir(parents=True)
    external.mkdir(parents=True)
    results_path = processed / "election_results_long.csv"
    pd.DataFrame(
        [
            {
                "form_type": "5_16",
                "polling_station_no": 1,
                "source_pdf": "advance.pdf",
                "source_page": 1,
                "choice_no": 1,
                "votes": 5,
                "valid_votes": 5,
            },
            {
                "form_type": "5_18",
                "polling_station_no": 1,
                "source_pdf": "station.pdf",
                "source_page": 1,
                "choice_no": 1,
                "votes": 20,
                "valid_votes": 27,
            },
            {
                "form_type": "5_18",
                "polling_station_no": 1,
                "source_pdf": "station.pdf",
                "source_page": 1,
                "choice_no": 2,
                "votes": 7,
                "valid_votes": 27,
            },
            {
                "form_type": "5_18_partylist",
                "polling_station_no": 1,
                "source_pdf": "party.pdf",
                "source_page": 1,
                "choice_no": 3,
                "votes": 8,
            },
        ]
    ).to_csv(results_path, index=False)
    before = results_path.read_text(encoding="utf-8")
    pd.DataFrame(
        [
            {
                "ballot_type": "constituency",
                "field": "votes",
                "choice_no": 1,
                "expected_value": 25,
                "source_form": "ส.ส. 6/1",
            },
            {
                "ballot_type": "constituency",
                "field": "votes",
                "choice_no": 2,
                "expected_value": 8,
                "source_form": "ส.ส. 6/1",
            },
            {
                "ballot_type": "party_list",
                "field": "votes",
                "choice_no": 3,
                "expected_value": 8,
                "source_form": "ส.ส. 6/1 (บช.)",
            },
            {
                "ballot_type": "constituency",
                "field": "valid_votes",
                "choice_no": "",
                "expected_value": 32,
                "source_form": "ส.ส. 6/1",
            },
        ]
    ).to_csv(external / "aggregate_validation_reference.csv", index=False)

    csv_path, md_path = write_aggregate_validation_report(_config(tmp_path))

    report = pd.read_csv(csv_path).fillna("")
    statuses = dict(zip(report["choice_no"].astype(str), report["status"], strict=False))
    assert statuses["1.0"] == "validated"
    assert statuses["2.0"] == "discrepancy"
    summary = report[report["field"].eq("valid_votes")].iloc[0]
    assert int(summary["actual_value"]) == 32
    assert summary["status"] == "validated"
    assert "overwrite" in md_path.read_text(encoding="utf-8")
    assert results_path.read_text(encoding="utf-8") == before


def test_aggregate_validation_reports_missing_optional_reference(tmp_path: Path):
    processed = tmp_path / "data/processed"
    processed.mkdir(parents=True)
    pd.DataFrame(
        [{"form_type": "5_18", "choice_no": 1, "votes": 10}]
    ).to_csv(processed / "election_results_long.csv", index=False)

    report = build_aggregate_validation_report(_config(tmp_path))

    assert report.loc[0, "status"] == "missing_reference"
