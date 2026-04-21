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
