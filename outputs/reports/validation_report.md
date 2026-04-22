# Validation Report

| Check | Status | Severity | Details |
|---|---|---|---|
| required_pdf_files | pass | critical | All required manifest PDFs are present |
| parsed_rows | pass | critical | 9,927 parsed result rows |
| required_forms_present | pass | critical | All required forms found |
| 5_18_station_coverage | fail | critical | 318 / 341 polling stations parsed |
| 5_18_partylist_station_coverage | fail | critical | 323 / 341 polling stations parsed |
| non_negative_votes | pass | critical | 0 rows have negative votes |
| duplicate_choice_rows | fail | critical | 1498 duplicate rows by form + station + choice; rows without station number are excluded |
| choice_votes_not_over_valid_votes | pass | major | 0 station/form groups exceed valid_votes |
| ballot_accounting | pass | major | 0 rows have valid + invalid + no_vote != ballots_cast |
| needs_review_rows | warn | major | 7206 rows require manual review |
