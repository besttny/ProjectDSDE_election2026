RESULT_COLUMNS = [
    "province",
    "constituency_no",
    "form_type",
    "vote_type",
    "polling_station_no",
    "district",
    "subdistrict",
    "choice_no",
    "choice_name",
    "party_name",
    "votes",
    "eligible_voters",
    "ballots_cast",
    "valid_votes",
    "invalid_votes",
    "no_vote",
    "source_pdf",
    "source_page",
    "ocr_engine",
    "ocr_confidence",
    "validation_status",
]

NUMERIC_COLUMNS = [
    "constituency_no",
    "polling_station_no",
    "choice_no",
    "votes",
    "eligible_voters",
    "ballots_cast",
    "valid_votes",
    "invalid_votes",
    "no_vote",
    "source_page",
    "ocr_confidence",
]

REQUIRED_FORMS = [
    "5_16",
    "5_16_partylist",
    "5_17",
    "5_17_partylist",
    "5_18",
    "5_18_partylist",
]

ELECTION_DAY_FORMS = ["5_18", "5_18_partylist"]

