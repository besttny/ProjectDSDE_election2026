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
    assert report.loc["5_18_source_evidence_coverage", "status"] == "fail"
    assert report.loc["duplicate_choice_rows", "status"] == "fail"
    assert report.loc["ballot_accounting", "status"] == "fail"
    assert report.loc["needs_review_rows", "status"] == "warn"


def test_validate_dataframe_keeps_source_evidence_separate_from_station_coverage():
    df = pd.DataFrame(
        [
            {
                "form_type": "5_18",
                "polling_station_no": 1,
                "choice_no": 1,
                "votes": 10,
                "valid_votes": 10,
                "source_pdf": "sample.pdf",
                "source_page": 1,
                "validation_status": "ok",
            },
            {
                "form_type": "5_18",
                "polling_station_no": 2,
                "choice_no": 1,
                "votes": "",
                "valid_votes": "",
                "source_pdf": "",
                "source_page": "",
                "validation_status": "needs_review",
            },
        ]
    )

    report = validate_dataframe(
        df,
        manifest_entries=[],
        expected_polling_stations=2,
    ).set_index("check")

    assert report.loc["5_18_station_coverage", "status"] == "pass"
    assert report.loc["5_18_source_evidence_coverage", "status"] == "fail"
    assert "1 / 2" in report.loc["5_18_source_evidence_coverage", "details"]


def test_validate_dataframe_flags_complete_choice_totals_that_do_not_match_valid_votes():
    df = pd.DataFrame(
        [
            {
                "form_type": "5_18",
                "polling_station_no": 1,
                "choice_no": 1,
                "votes": 40,
                "valid_votes": 100,
                "source_pdf": "sample.pdf",
                "source_page": 1,
                "validation_status": "ok",
            },
            {
                "form_type": "5_18",
                "polling_station_no": 1,
                "choice_no": 2,
                "votes": 50,
                "valid_votes": 100,
                "source_pdf": "sample.pdf",
                "source_page": 1,
                "validation_status": "ok",
            },
        ]
    )

    report = validate_dataframe(
        df,
        manifest_entries=[],
        expected_polling_stations=1,
    ).set_index("check")

    assert report.loc["choice_votes_not_over_valid_votes", "status"] == "pass"
    assert report.loc["choice_votes_match_valid_votes", "status"] == "fail"
    assert "1 complete station/form groups" in report.loc["choice_votes_match_valid_votes", "details"]


def test_validate_dataframe_flags_implausible_vote_cells():
    df = pd.DataFrame(
        [
            {
                "form_type": "5_18",
                "polling_station_no": 1,
                "choice_no": 1,
                "votes": 1000,
                "validation_status": "ok",
            }
        ]
    )

    report = validate_dataframe(
        df,
        manifest_entries=[],
        expected_polling_stations=1,
        max_vote_cell_value=999,
    ).set_index("check")

    assert report.loc["vote_cell_value_plausibility", "status"] == "fail"
    assert "above 999" in report.loc["vote_cell_value_plausibility", "details"]


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


def test_validate_dataframe_reports_result_rows_not_matching_master_keys(tmp_path: Path):
    candidate_master = tmp_path / "master_candidates.csv"
    candidate_master.write_text(
        "province,constituency_no,form_type,candidate_no,canonical_name,party_name,aliases\n"
        "ชัยภูมิ,2,518,1,นาย ก,พรรค ก,\n",
        encoding="utf-8",
    )
    party_master = tmp_path / "master_parties.csv"
    party_master.write_text(
        "party_no,canonical_name,aliases\n"
        "1,พรรคหนึ่ง,\n",
        encoding="utf-8",
    )
    df = pd.DataFrame(
        [
            {
                "province": "ชัยภูมิ",
                "constituency_no": 2,
                "form_type": "5_18",
                "choice_no": 1,
                "validation_status": "ok",
            },
            {
                "province": "ชัยภูมิ",
                "constituency_no": 2,
                "form_type": "5_18",
                "choice_no": 9,
                "validation_status": "ok",
            },
            {
                "province": "ชัยภูมิ",
                "constituency_no": 2,
                "form_type": "5_18_partylist",
                "choice_no": 99,
                "validation_status": "needs_review",
            },
        ]
    )

    report = validate_dataframe(
        df,
        manifest_entries=[],
        expected_polling_stations=341,
        candidate_master_path=candidate_master,
        party_master_path=party_master,
    ).set_index("check")

    assert report.loc["result_candidate_master_matches", "status"] == "fail"
    assert "1 are still marked ok" in report.loc["result_candidate_master_matches", "details"]
    assert report.loc["result_party_master_matches", "status"] == "warn"
    assert "0 are still marked ok" in report.loc["result_party_master_matches", "details"]
