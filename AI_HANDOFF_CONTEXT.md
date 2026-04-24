# AI Handoff Context - Election 2026 OCR Project

Last updated: 2026-04-24

This file is for handing the project to another person or a new AI/Codex chat.
It summarizes the current project context, election-domain rules, latest
pipeline status, known problems, and recommended next steps.

## Project Scope

- Project: DSDE Election 2026 OCR/data pipeline
- Config: `configs/chaiyaphum_2.yaml`
- Province: `ชัยภูมิ`
- Constituency: `2`
- Expected polling stations: `341`
- Main manifest: `configs/chaiyaphum_2_manifest.csv`
- Main source URL in config/manifest: `https://www.ect.go.th/ect_th/th/election-2026`

Main local rebuild command after OCR artifacts already exist:

```bash
python -m src.pipeline.run_all --config configs/chaiyaphum_2.yaml --skip-ocr
```

Full OCR is intended to run on Google Colab/A100. Local runs should normally
use `--skip-ocr` after copying OCR artifacts back.

## Election-Domain Rules

- The primary dataset must be derived from OCR/source-page review of official
  ECT PDFs for `5_16`, `5_16_partylist`, `5_17`, `5_17_partylist`, `5_18`,
  and `5_18_partylist`.
- `ส.ส. 6/1` and `ส.ส. 6/1 (บช.)` are aggregate validation references only.
  Use them to compare constituency-level sums and flag discrepancies. Never
  overwrite row-level OCR or reviewed values from `6/1` totals automatically.
  If totals differ, go back to the relevant `5/16`-`5/18` source PDF page,
  crop, raw OCR JSON, and review queue item.
- Optional aggregate references belong in
  `data/external/aggregate_validation_reference.csv`; the report writer
  `src.quality.aggregate_validation` emits discrepancy evidence only and does
  not mutate `election_results_long.csv`, final CSVs, or reviewed data.
- Constituency ballot (`ส.ส.เขต`) candidate numbers are constituency-scoped.
  The same `candidate_no` can refer to different people in different
  constituencies.
- One constituency candidate should not appear in more than one constituency.
- Therefore constituency candidate identity should use:
  `province + constituency_no + candidate_no`
- Party-list ballot (`บัญชีรายชื่อ`) numbers are nationwide party numbers.
  Party-list identity should use `party_no`.

Supporting references:

- ECT organic law page:
  `https://www.ect.go.th/ect_th/th/organic-act`
- ECT Organic Act PDF:
  `https://www.ect.go.th/web-upload/migrate/download/article/article_20180913155522-copy70.pdf`
- ThaiPBS summary of 57 party-list numbers from ECT announcement:
  `https://www.thaipbs.or.th/news/content/501553`

## Important Files

- `src/pipeline/clean.py`
  - `apply_master_names()` maps constituency candidates by
    `province + constituency_no + candidate_no`.
  - Party-list rows map by `party_no`.

- `src/pipeline/validate.py`
  - Adds candidate master checks:
    - `candidate_master_schema`
    - `candidate_master_scoped_keys`
    - `candidate_master_one_person_one_constituency`
    - `candidate_master_candidate_no_scoped`
  - Separates parsed station coverage from source evidence coverage:
    - `5_18_station_coverage`
    - `5_18_source_evidence_coverage`
    - `5_18_partylist_station_coverage`
    - `5_18_partylist_source_evidence_coverage`
  - Adds exact complete-group check:
    - `choice_votes_match_valid_votes`

- `src/pipeline/expected_rows.py`
  - Scaffolds missing expected 5/18 rows from official candidate/party masters.
  - Missing source/vote rows stay `needs_review` and are not silently accepted.

- `src/quality/review_queue.py`
  - Adds P0 reason `missing_source_page`.
  - Adds P0 reason `choice_votes_mismatch_valid_votes`.

- `src/pipeline/station_inference.py`
  - Infers station/form from `source_pdf + source_page`, important for mixed
    `5_18_auto` PDFs.
  - Must not overwrite `polling_station_no` on `manual_review` rows.

- `src/quality/evaluate_accuracy.py`
  - Canonicalizes candidate names through `master_candidates.csv` aliases
    before comparing ground truth with final outputs.

- `data/external/master_candidates.csv`
  - Columns:
    `province,constituency_no,form_type,candidate_no,canonical_name,party_name,aliases,source_pdf,source_page,source_note`
  - Currently has 7 candidates for `ชัยภูมิ เขต 2`.

- `data/external/master_parties.csv`
  - Contains 57 party-list numbers.

- `data/processed/validation_report.csv`
  - Latest validation summary.

- `data/processed/review_queue.csv`
  - Manual review queue.

## Latest Output Status

Latest `run_all --skip-ocr` completed successfully.
Latest imported Colab OCR artifact:
`data/ocr_all_fresh_artifacts_v3.zip`

Artifact SHA-256:
`a9b50a76a8eb122a86882ffc6e26a72a34f996640bdcf8ef7efde6dabf630de6`

Parsed rows:

- Total: `21,903`
- `5_18_partylist`: `19,437`
- `5_18`: `2,387`
- `5_17`: `28`
- `5_17_partylist`: `28`
- `5_16_partylist`: `21`
- `5_16`: `2`

Validation summary:

- Required PDFs: pass
- Parsed rows: pass
- Required forms present: pass
- Candidate master schema/scoped key checks: pass
- Candidate one-person-one-constituency check: pass
- Result candidate master match: warn, `9` unmatched constituency rows;
  `0` are still marked `ok`
- Result party master match: pass
- Thai text charset: warn, `8` rows; `0` are still marked `ok`
- `5_18` station coverage: pass, `341 / 341`
- `5_18` source evidence coverage: fail, `339 / 341`
- `5_18_partylist` station coverage: pass, `341 / 341`
- `5_18_partylist` source evidence coverage: fail, `323 / 341`
- Negative votes: pass
- Duplicate result rows: pass, `0`
- Vote totals over valid votes: pass
- Exact complete-group choice-vote totals: pass
- Ballot accounting: pass
- Needs review rows: warn, `18,245`

Aggregate validation:

- Official `ส.ส. 6/1` / `ส.ส. 6/1 (บช.)` reference is now present in
  `data/external/aggregate_validation_reference.csv`.
- Reference rows: `74` total (`12` constituency, `62` party-list).
- Current aggregate report after rebuilding without rerunning OCR:
  `70` discrepancies and `4` missing actual aggregate fields.
- The aggregate reference is only for validation/flagging; do not overwrite
  unit-level OCR values with `6/1` totals.

Accuracy report:

- Ground truth rows: `84 / 84`
- Overall field accuracy: `504 / 504 = 1.0`
- Row exact accuracy: `84 / 84 = 1.0`
- Candidate-name comparison is alias-aware, so official canonical names from
  `master_candidates.csv` are not counted wrong when older ground-truth rows
  contain known aliases.
- Important caveat: this is sample accuracy only. Do not claim full-district
  99% until more ground truth rows and P0 source/vote issues are reviewed.

Review queue:

- Total rows: `40,122`
- `P0`: `19,460`
- `P1`: `20,662`
- Top reasons:
  - `parser_marked_needs_review`: `18,245`
  - `missing_votes`: `18,232`
  - `master_data_unmatched`: `1,993`
  - `missing_source_page`: `1,228`
  - `low_ocr_confidence`: `416`
  - `invalid_text_charset`: `8`

P0 fallback targets:

- Total targets: `2,312`
- `5_18`: `510` targets
- `5_18_partylist`: `1,786` targets
- Advance-form targets: `16` targets across `5_16`, `5_16_partylist`,
  `5_17`, and `5_17_partylist`
- Missing source stations:
  - `5_18`: `340, 341`
  - `5_18_partylist`:
    `101, 118, 133, 203, 236, 247, 326, 327, 328, 329, 330, 331, 332, 333,
    334, 335, 336, 337, 338, 339, 340, 341`

Manual review inputs:

- `data/external/reviewed_vote_cells.csv`: `17` visually confirmed digit-crop cells
  from `5_18` pages.
- `data/external/reviewed_rows.csv`: `84` manually reviewed constituency rows.
- `data/external/ground_truth_sample.csv`: `84` ground-truth rows across
  `12` reviewed constituency station samples.
- Station inference must preserve manually reviewed station IDs. A regression
  test now covers this because v3 exposed a bug where inference could shift
  reviewed station IDs after `reviewed_rows.csv` was applied.
- Newly added manually verified constituency stations in this round:
  `4, 126, 154, 215, 288`

Master data:

- `master_candidates.csv`: `7` rows, now sourced from the official ECT
  `ส.ส. 6/1` constituency aggregate candidate table.
- `master_parties.csv`: `57` rows, `57` unique party numbers, now sourced
  from the official ECT `ส.ส. 6/1 (บช.)` party-list aggregate table.
- Official reference/data completeness details are documented in
  `docs/reference_data_audit.md`.

## Known Problems

1. Parsed station coverage is now complete, but source evidence is not.
   - `5_18`: `339 / 341` stations have source PDF/page evidence
   - `5_18_partylist`: `323 / 341` stations have source PDF/page evidence
   - Rows without source evidence are scaffold rows and must stay
     `needs_review` until a real PDF/page and vote values are confirmed.

2. Many rows still need review.
   - Current `needs_review_rows`: `18,245`
   - Current P0 rows: `19,460`

3. Current sample accuracy is perfect but too small for a final claim.
   - `ground_truth_sample.csv` has `84` rows.
   - Add more reviewed units from different PDFs/subdistricts before claiming
     full 99% accuracy.

4. Some OCR/parser noise still creates unmatched candidate rows, but none of
   those rows are marked `ok`.
   - Current `result_candidate_master_matches`: warn, `9` rows unmatched.

5. For higher accuracy, do not OCR candidate/party names repeatedly.
   - Names should come from master files.
   - OCR should focus on `station_id`, numeric vote cells, and summary totals.
   - Recommended next OCR improvement is cell-crop digits OCR for P0/missing
     vote cells.

6. Current `missing_source_page` gaps now look like source-asset coverage gaps,
   not just parser misses.
   - The unresolved targets are currently:
     - `5_18`: `340, 341`
     - `5_18_partylist`: `101, 118, 133, 203, 236, 247, 326-341`
   - Fixed-form source PDFs at the end of the manifest do not currently have
     enough pages to cover the expected station counts if you assume the normal
     `2 pages/station` for `5_18` and `4 pages/station` for `5_18_partylist`.
   - Concrete current evidence from raw OCR page counts:
     - `ตาเนิน-001-บัญชีรายชื่อ.PDF`: `52` pages -> `13` assigned stations
       while `ตาเนิน-001-แบ่งเขต.PDF` has `32` pages -> `16` assigned stations
     - `รังงาม-001-บัญชีรายชื่อ.PDF`: `28` pages -> `7` assigned stations
       while `รังงาม-001-แบ่งเขต.PDF` has `20` pages -> `10` assigned stations
     - `หนองฉิม-001-บัญชีรายชื่อ.PDF`: `60` pages -> `15` assigned stations
       and `หนองฉิม-001-แบ่งเขต.PDF`: `30` pages -> `15` assigned stations
   - This means the remaining `5_18_partylist 326-341` gap cannot be recovered
     honestly from the current extracted PDF set unless more official
     pages/files are found.

## Recommended Next Steps

1. Start with `data/processed/p0_fallback_targets.csv`.
   - Continue with `missing_votes` targets that already have source PDF/page.
   - Use `data/processed/digit_crop_ocr_suggestions.csv` as a shortlist, but
     visually verify each crop before copying anything into reviewed data.
2. Treat `missing_source_page` in two buckets:
   - Individual `5_18_partylist` gaps now at stations
     `101, 118, 133, 203, 236, 247`; inspect source-page assignment before
     adding manual rows.
   - `5_18 340-341` and `5_18_partylist 326-341`: likely blocked by missing
     source pages/files in the current extracted PDF set; verify against
     official source or prepared Google Drive zip before editing inference again
3. For confirmed pages, crop vote cells or use Google Vision/manual review and
   record corrections in:
   - `data/external/reviewed_vote_cells.csv`
   - `data/external/reviewed_rows.csv`
4. Add 5-10 more ground-truth station samples from different PDFs/subdistricts
   to `data/external/ground_truth_sample.csv`.
5. Re-run:

```bash
python -m src.pipeline.run_all --config configs/chaiyaphum_2.yaml --skip-ocr
```

6. Review:

```bash
data/processed/validation_report.csv
data/processed/review_queue.csv
data/processed/constituency_votes.csv
data/processed/partylist_votes.csv
```

## Suggested Prompt For New AI Chat

Read `AI_HANDOFF_CONTEXT.md` and continue from the recommended next steps.
First, inspect `data/processed/p0_fallback_targets.csv` and continue with P0
`missing_votes` rows that already have source PDF/page. Use
`data/processed/digit_crop_ocr_suggestions.csv` plus crop images for manual
verification, copy confirmed values into `data/external/reviewed_vote_cells.csv`,
rerun `run_all --skip-ocr`, and report review queue deltas. Separately audit the
remaining `missing_source_page` rows and distinguish parser bugs from genuinely
missing/incomplete source PDFs before changing station inference again.
