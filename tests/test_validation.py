from pathlib import Path

import pandas as pd

from src.pipeline.manifest import ManifestEntry
from src.pipeline.validate import validate_dataframe


def test_validate_dataframe_flags_duplicates_and_missing_station_coverage():
    df = pd.DataFrame(
        [
            {
                "form_type": "5_18",
                "polling_station_no": 1,
                "choice_no": 1,
                "votes": 10,
                "ballots_cast": 120,
                "valid_votes": 100,
                "invalid_votes": 10,
                "no_vote": 10,
                "validation_status": "ok",
            },
            {
                "form_type": "5_18",
                "polling_station_no": 1,
                "choice_no": 1,
                "votes": 20,
                "ballots_cast": 130,
                "valid_votes": 100,
                "invalid_votes": 10,
                "no_vote": 10,
                "validation_status": "needs_review",
            },
        ]
    )
    manifest_entries = [
        ManifestEntry("5_18", "constituency", True, 341, Path("missing.pdf"), "", "")
    ]

    report = validate_dataframe(
        df,
        manifest_entries=manifest_entries,
        expected_polling_stations=341,
    ).set_index("check")

    assert report.loc["required_pdf_files", "status"] == "fail"
    assert report.loc["5_18_station_coverage", "status"] == "fail"
    assert report.loc["duplicate_choice_rows", "status"] == "fail"
    assert report.loc["ballot_accounting", "status"] == "fail"
    assert report.loc["needs_review_rows", "status"] == "warn"


def test_validate_dataframe_checks_candidate_master_scope_and_one_person_rule(tmp_path: Path):
    candidate_master = tmp_path / "master_candidates.csv"
    candidate_master.write_text(
        "province,constituency_no,form_type,candidate_no,canonical_name,party_name,aliases\n"
        "ชัยภูมิ,2,518,1,นาย ก,พรรค ก,\n"
        "ชัยภูมิ,2,518,1,นาย ข,พรรค ข,\n"
        "ชัยภูมิ,3,518,1,นาย ค,พรรค ค,\n"
        "ชัยภูมิ,4,518,4,นาย ก,พรรค ง,\n",
        encoding="utf-8",
    )

    report = validate_dataframe(
        pd.DataFrame(),
        manifest_entries=[],
        expected_polling_stations=341,
        candidate_master_path=candidate_master,
    ).set_index("check")

    assert report.loc["candidate_master_schema", "status"] == "pass"
    assert report.loc["candidate_master_scoped_keys", "status"] == "fail"
    assert report.loc["candidate_master_one_person_one_constituency", "status"] == "fail"
    assert report.loc["candidate_master_candidate_no_scoped", "status"] == "pass"
