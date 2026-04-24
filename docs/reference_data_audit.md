# Reference Data Audit

Last checked: 2026-04-24

This project must treat unit-level OCR from the official `ส.ส. 5/16`,
`ส.ส. 5/17`, and `ส.ส. 5/18` PDF forms as the primary data. Aggregate forms
`ส.ส. 6/1` and `ส.ส. 6/1 (บช.)` are validation references only and must not
overwrite OCR rows automatically.

## Complete Or Usable

- Primary OCR PDFs: complete for the current Chaiyaphum constituency 2 manifest.
  `configs/chaiyaphum_2_manifest.csv` lists 35 required PDFs, and all 35 are
  present locally under `data/raw/pdfs/extracted/`.
- Official aggregate reference: complete for Chaiyaphum constituency 2.
  `data/external/aggregate_validation_reference.csv` contains 74 official
  reference rows from ECT `ส.ส. 6/1` / `ส.ส. 6/1 (บช.)`: 12 constituency rows
  and 62 party-list rows.
- Candidate master: usable. `data/external/master_candidates.csv` contains the
  7 constituency candidates from the official ECT `ส.ส. 6/1` candidate table.
- Party master: usable. `data/external/master_parties.csv` contains all 57
  party-list numbers, now sourced from the official ECT `ส.ส. 6/1 (บช.)`
  aggregate table.
- Manual correction file: present at `data/external/manual_corrections.csv`,
  but intentionally empty until a reviewer approves corrections.

## Incomplete Ground Truth

- Field-level ground truth is not complete. `data/external/ground_truth_sample.csv`
  has 84 rows covering only `5_18` constituency samples: 12 polling stations
  x 7 candidates. It has no reviewed ground truth for `5_16`, `5_16_partylist`,
  `5_17`, `5_17_partylist`, or `5_18_partylist`.
- Reviewed rows are not complete. `data/external/reviewed_rows.csv` also covers
  only the same 84 `5_18` constituency rows.
- Reviewed vote cells are not complete. `data/external/reviewed_vote_cells.csv`
  has 17 reviewed vote cells, all from `5_18`.
- Polling station master is not a full official 341-station ground truth file.
  `data/external/master_polling_stations.csv` currently has 22 area/PDF-level
  rows and blank `station_id` values; it is useful for source orientation, not
  for station-level ground truth validation.

## Current Validation Signal

- `data/processed/aggregate_validation_report.csv` now compares OCR-derived
  aggregate totals with official `ส.ส. 6/1` references.
- Current status after rebuilding without rerunning OCR: 69 discrepancies,
  4 missing actual aggregate fields, and 1 validated row.
- This means the aggregate reference data is now available, but the OCR output
  still needs targeted reruns and manual review. The discrepancy report should
  guide which forms, pages, stations, and fields to inspect first.

## Official Sources Added

- ECT announcement page:
  `https://www.ect.go.th/ect_th/th/db_119_ect_th_cms_1/8116`
- Chaiyaphum constituency 2 `ส.ส. 6/1`:
  `https://drive.google.com/file/d/1wDmWd3fSbSXlPvalTgH_of6TOKkUdJk9/view`
- Chaiyaphum constituency 2 `ส.ส. 6/1 (บช.)`:
  `https://drive.google.com/file/d/1B2BqUkGnl5jJdaO-QKXtR6KTl7Sukr08/view`
