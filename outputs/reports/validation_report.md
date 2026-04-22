# Validation Report

| Check | Status | Severity | Details |
|---|---|---|---|
| required_pdf_files | pass | critical | All required manifest PDFs are present |
| parsed_rows | pass | critical | 20,570 parsed result rows |
| required_forms_present | pass | critical | All required forms found |
| candidate_master_schema | pass | critical | Candidate master is scoped by province + constituency_no + candidate_no |
| candidate_master_scoped_keys | pass | critical | 0 rows have incomplete scope; 0 rows duplicate province + constituency_no + candidate_no |
| candidate_master_one_person_one_constituency | pass | critical | 0 candidate names appear in more than one province + constituency_no |
| candidate_master_candidate_no_scoped | pass | major | 0 candidate numbers are reused across constituencies; mapping uses province + constituency_no + candidate_no, not candidate_no alone |
| result_candidate_master_matches | warn | major | 10 constituency result rows are not in candidate master; 0 are still marked ok |
| result_party_master_matches | pass | major | 0 party-list result rows are not in party master; 0 are still marked ok |
| thai_text_charset | warn | major | 10 rows contain non-Thai text in Thai fields; 0 are still marked ok |
| 5_18_station_coverage | fail | critical | 338 / 341 polling stations parsed |
| 5_18_partylist_station_coverage | fail | critical | 318 / 341 polling stations parsed |
| non_negative_votes | pass | critical | 0 rows have negative votes |
| duplicate_choice_rows | pass | critical | 0 duplicate rows by form + station + choice; rows without station number are excluded |
| choice_votes_not_over_valid_votes | pass | major | 0 station/form groups exceed valid_votes |
| ballot_accounting | pass | major | 0 rows have valid + invalid + no_vote != ballots_cast |
| needs_review_rows | warn | major | 17613 rows require manual review |
