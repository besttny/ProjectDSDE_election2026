import pandas as pd

from src.pipeline.final_export import (
    CONSTITUENCY_COLUMNS,
    PARTYLIST_COLUMNS,
    build_constituency_votes,
    build_partylist_votes,
)


def test_build_final_constituency_schema_headers_and_values():
    df = pd.DataFrame(
        [
            {
                "province": "ชัยภูมิ",
                "constituency_no": 2,
                "form_type": "5_18",
                "polling_station_no": 2401002,
                "district": "อำเภอเมือง",
                "subdistrict": "ที่ว่าการ",
                "choice_no": 3,
                "choice_name": "นายสมชาย",
                "party_name": "เพื่อไทย",
                "votes": 312,
                "ballots_cast": 650,
                "invalid_votes": 8,
                "no_vote": 15,
            }
        ]
    )

    output = build_constituency_votes(df)

    assert list(output.columns) == CONSTITUENCY_COLUMNS
    assert output.loc[0, "zone"] == "2"
    assert output.loc[0, "form_type"] == "518"
    assert output.loc[0, "voting_type"] == "election_day"
    assert output.loc[0, "station_id"] == "2401002"
    assert output.loc[0, "candidate_no"] == "3"
    assert output.loc[0, "total_ballots"] == "650"
    assert output.loc[0, "invalid_ballots"] == "8"
    assert output.loc[0, "no_vote_ballots"] == "15"


def test_build_final_partylist_schema_headers_and_values():
    df = pd.DataFrame(
        [
            {
                "province": "ชัยภูมิ",
                "constituency_no": 2,
                "form_type": "5_18_partylist",
                "polling_station_no": 2401002,
                "choice_no": 5,
                "choice_name": "",
                "party_name": "เพื่อไทย",
                "votes": 289,
                "ballots_cast": 650,
                "invalid_votes": 10,
                "no_vote": 12,
            }
        ]
    )

    output = build_partylist_votes(df)

    assert list(output.columns) == PARTYLIST_COLUMNS
    assert output.loc[0, "zone"] == "2"
    assert output.loc[0, "form_type"] == "518_bch"
    assert output.loc[0, "voting_type"] == "election_day"
    assert output.loc[0, "party_no"] == "5"
    assert output.loc[0, "party"] == "เพื่อไทย"
