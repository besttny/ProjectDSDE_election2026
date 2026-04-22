# Validation Report

| Check | Status | Severity | Details |
|---|---|---|---|
| required_pdf_files | pass | critical | All required manifest PDFs are present |
| parsed_rows | pass | critical | 8,916 parsed result rows |
| required_forms_present | pass | critical | All required forms found |
| candidate_master_schema | pass | critical | Candidate master is scoped by province + constituency_no + candidate_no |
| candidate_master_scoped_keys | pass | critical | 0 rows have incomplete scope; 0 rows duplicate province + constituency_no + candidate_no |
| candidate_master_one_person_one_constituency | pass | critical | 0 candidate names appear in more than one province + constituency_no |
| candidate_master_candidate_no_scoped | pass | major | 0 candidate numbers are reused across constituencies; mapping uses province + constituency_no + candidate_no, not candidate_no alone |
| 5_18_station_coverage | fail | critical | 329 / 341 polling stations parsed |
| 5_18_partylist_station_coverage | fail | critical | 315 / 341 polling stations parsed |
| non_negative_votes | pass | critical | 0 rows have negative votes |
| duplicate_choice_rows | pass | critical | 0 duplicate rows by form + station + choice; rows without station number are excluded |
| choice_votes_not_over_valid_votes | pass | major | 0 station/form groups exceed valid_votes |
| ballot_accounting | pass | major | 0 rows have valid + invalid + no_vote != ballots_cast |
| needs_review_rows | warn | major | 6358 rows require manual review |
