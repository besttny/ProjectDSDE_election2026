# Validation Report

| Check | Status | Severity | Details |
|---|---|---|---|
| required_pdf_files | fail | critical | 6 required PDFs missing |
| parsed_rows | fail | critical | 0 parsed result rows |
| required_forms_present | fail | critical | Missing forms: 5_16, 5_16_partylist, 5_17, 5_17_partylist, 5_18, 5_18_partylist |
| 5_18_station_coverage | fail | critical | 0 / 341 polling stations parsed |
| 5_18_partylist_station_coverage | fail | critical | 0 / 341 polling stations parsed |
| non_negative_votes | pass | critical | 0 rows have negative votes |
| duplicate_choice_rows | pass | critical | 0 duplicate rows by form + station + choice |
| choice_votes_not_over_valid_votes | warn | major | Required columns unavailable for total comparison |
| needs_review_rows | pass | major | 0 rows require manual review |
