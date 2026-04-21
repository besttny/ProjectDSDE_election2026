from pathlib import Path

from src.pipeline.config import ProjectConfig
from src.quality.review_queue import build_review_queue


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
                "master_parties_file": "data/external/master_parties.csv",
                "master_candidates_file": "data/external/master_candidates.csv",
                "master_stations_file": "data/external/master_polling_stations.csv",
            },
            "outputs": {
                "election_results": "data/processed/election_results_long.csv",
                "polling_station_summary": "data/processed/polling_station_summary.csv",
                "review_queue": "data/processed/review_queue.csv",
                "master_match_report": "data/processed/master_match_report.csv",
            },
            "ocr": {"confidence_threshold": 0.65},
            "quality": {"fuzzy_match_threshold": 0.86},
        },
    )


def test_build_review_queue_flags_rows_that_block_high_accuracy(tmp_path: Path):
    config = _config(tmp_path)
    results_path = tmp_path / "data/processed/election_results_long.csv"
    results_path.parent.mkdir(parents=True)
    results_path.write_text(
        "province,constituency_no,form_type,vote_type,polling_station_no,district,subdistrict,"
        "choice_no,choice_name,party_name,votes,eligible_voters,ballots_cast,valid_votes,"
        "invalid_votes,no_vote,source_pdf,source_page,ocr_engine,ocr_confidence,validation_status\n"
        "ชัยภูมิ,2,5_18,constituency,2401002,เมือง,ในเมือง,3,นายสมชาย,เพื่อไทย,,"
        "800,650,627,8,15,sample.pdf,1,paddleocr,0.45,needs_review\n",
        encoding="utf-8",
    )
    external = tmp_path / "data/external"
    external.mkdir(parents=True)
    (external / "master_parties.csv").write_text("party_no,canonical_name,aliases\n", encoding="utf-8")
    (external / "master_candidates.csv").write_text("form_type,candidate_no,canonical_name,party_name,aliases\n", encoding="utf-8")
    (external / "master_polling_stations.csv").write_text("station_id,station_name,district,subdistrict,aliases\n", encoding="utf-8")

    queue = build_review_queue(config)
    reasons = set(queue["reason"])

    assert {"missing_votes", "low_ocr_confidence", "parser_marked_needs_review"}.issubset(reasons)
    assert queue.iloc[0]["priority"] == "P0"
