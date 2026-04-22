from pathlib import Path

import pandas as pd

from src.pipeline.config import ProjectConfig
from src.quality.invalid_text_review import build_invalid_text_review


def _config(root: Path) -> ProjectConfig:
    return ProjectConfig(
        root=root,
        data={
            "paths": {
                "processed_dir": "data/processed",
                "raw_image_dir": "data/raw/images",
                "raw_ocr_dir": "data/raw/ocr",
                "parsed_dir": "data/processed/parsed",
                "figures_dir": "outputs/figures",
                "reports_dir": "outputs/reports",
                "master_candidates_file": "data/external/master_candidates.csv",
                "master_parties_file": "data/external/master_parties.csv",
            },
            "outputs": {
                "review_queue": "data/processed/review_queue.csv",
            },
        },
    )


def test_build_invalid_text_review_classifies_invalid_choice_before_digit_fallback(tmp_path: Path):
    external = tmp_path / "data/external"
    processed = tmp_path / "data/processed"
    external.mkdir(parents=True)
    processed.mkdir(parents=True)
    (external / "master_candidates.csv").write_text(
        "province,constituency_no,candidate_no,canonical_name,party_name\n"
        "ชัยภูมิ,2,1,นาย ก,พรรค ก\n",
        encoding="utf-8",
    )
    (external / "master_parties.csv").write_text(
        "party_no,canonical_name\n"
        "1,พรรคบัญชีรายชื่อหนึ่ง\n",
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "priority": "P1",
                "reason": "invalid_text_charset",
                "row_index": 1,
                "form_type": "5_18",
                "vote_type": "constituency",
                "polling_station_no": 1,
                "choice_no": 7,
                "choice_name": "ABC",
                "party_name": "",
                "votes": "",
                "source_pdf": "sample.pdf",
                "source_page": 1,
            },
            {
                "priority": "P1",
                "reason": "invalid_text_charset",
                "row_index": 2,
                "form_type": "5_18_partylist",
                "vote_type": "party_list",
                "polling_station_no": 1,
                "choice_no": 99,
                "choice_name": "",
                "party_name": "WUG",
                "votes": 3,
                "source_pdf": "sample.pdf",
                "source_page": 1,
            },
        ]
    ).to_csv(processed / "review_queue.csv", index=False, encoding="utf-8-sig")

    review = build_invalid_text_review(_config(tmp_path))

    assert review.loc[0, "classification"] == "invalid_choice_number"
    assert bool(review.loc[0, "has_missing_votes"]) is True
    assert review.loc[1, "classification"] == "invalid_choice_number"
    assert bool(review.loc[1, "choice_in_master"]) is False


def test_build_invalid_text_review_classifies_valid_choice_missing_vote_as_digit_fallback(tmp_path: Path):
    external = tmp_path / "data/external"
    processed = tmp_path / "data/processed"
    external.mkdir(parents=True)
    processed.mkdir(parents=True)
    (external / "master_candidates.csv").write_text(
        "province,constituency_no,candidate_no,canonical_name,party_name\n"
        "ชัยภูมิ,2,1,นาย ก,พรรค ก\n",
        encoding="utf-8",
    )
    (external / "master_parties.csv").write_text(
        "party_no,canonical_name\n"
        "1,พรรคบัญชีรายชื่อหนึ่ง\n",
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "priority": "P1",
                "reason": "invalid_text_charset",
                "row_index": 1,
                "form_type": "5_18",
                "vote_type": "constituency",
                "polling_station_no": 1,
                "choice_no": 1,
                "choice_name": "ABC",
                "party_name": "",
                "votes": "",
                "source_pdf": "sample.pdf",
                "source_page": 1,
            },
        ]
    ).to_csv(processed / "review_queue.csv", index=False, encoding="utf-8-sig")

    review = build_invalid_text_review(_config(tmp_path))

    assert review.loc[0, "classification"] == "needs_digit_fallback"
    assert bool(review.loc[0, "choice_in_master"]) is True
