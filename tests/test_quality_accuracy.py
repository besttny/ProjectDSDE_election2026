from pathlib import Path

from src.pipeline.config import ProjectConfig
from src.quality.evaluate_accuracy import evaluate_accuracy


def _config(root: Path) -> ProjectConfig:
    return ProjectConfig(
        root=root,
        data={
            "paths": {
                "raw_image_dir": "data/raw/images",
                "raw_ocr_dir": "data/raw/ocr",
                "parsed_dir": "data/processed/parsed",
                "processed_dir": "data/processed",
                "external_dir": "data/external",
                "figures_dir": "outputs/figures",
                "reports_dir": "outputs/reports",
                "correction_file": "data/external/manual_corrections.csv",
                "ground_truth_file": "data/external/ground_truth_sample.csv",
            },
            "outputs": {
                "election_results": "data/processed/election_results_long.csv",
                "polling_station_summary": "data/processed/polling_station_summary.csv",
                "constituency_votes": "data/processed/constituency_votes.csv",
                "partylist_votes": "data/processed/partylist_votes.csv",
                "accuracy_report": "data/processed/accuracy_report.csv",
                "accuracy_details": "data/processed/accuracy_details.csv",
                "accuracy_report_md": "outputs/reports/accuracy_report.md",
            },
            "quality": {"target_accuracy": 0.99},
        },
    )


def test_evaluate_accuracy_compares_ground_truth_to_final_schema(tmp_path: Path):
    config = _config(tmp_path)
    results_path = tmp_path / "data/processed/election_results_long.csv"
    results_path.parent.mkdir(parents=True)
    results_path.write_text(
        "province,constituency_no,form_type,vote_type,polling_station_no,district,subdistrict,"
        "choice_no,choice_name,party_name,votes,eligible_voters,ballots_cast,valid_votes,"
        "invalid_votes,no_vote,source_pdf,source_page,ocr_engine,ocr_confidence,validation_status\n"
        "ชัยภูมิ,2,5_18,constituency,2401002,เมือง,ในเมือง,3,นายสมชาย,เพื่อไทย,312,"
        "800,650,627,8,15,sample.pdf,1,paddleocr,0.95,ok\n",
        encoding="utf-8",
    )

    ground_truth_path = tmp_path / "data/external/ground_truth_sample.csv"
    ground_truth_path.parent.mkdir(parents=True)
    ground_truth_path.write_text(
        "ballot_type,form_type,station_id,choice_no,votes,total_ballots,invalid_ballots,"
        "no_vote_ballots,name,party,source_page,reviewer_notes\n"
        "constituency,518,2401002,3,312,650,8,15,นายสมชาย,เพื่อไทย,1,checked\n",
        encoding="utf-8",
    )

    report, details = evaluate_accuracy(config)
    report = report.set_index("metric")

    assert report.loc["overall_field_accuracy", "status"] == "pass"
    assert report.loc["row_exact_accuracy", "accuracy"] == 1.0
    assert len(details) == 6


def test_evaluate_accuracy_warns_when_ground_truth_is_empty(tmp_path: Path):
    config = _config(tmp_path)
    ground_truth_path = tmp_path / "data/external/ground_truth_sample.csv"
    ground_truth_path.parent.mkdir(parents=True)
    ground_truth_path.write_text(
        "ballot_type,form_type,station_id,choice_no,votes,total_ballots,invalid_ballots,"
        "no_vote_ballots,name,party,source_page,reviewer_notes\n",
        encoding="utf-8",
    )

    report, details = evaluate_accuracy(config)

    assert report.loc[0, "metric"] == "ground_truth_rows"
    assert report.loc[0, "status"] == "warn"
    assert details.empty
