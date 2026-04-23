# AI Handoff Context - Election 2026 OCR Project

Last updated: 2026-04-23

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

Parsed rows:

- Total: `21,902`
- `5_18_partylist`: `19,437`
- `5_18`: `2,387`
- `5_17_partylist`: `29`
- `5_17`: `17`
- `5_16_partylist`: `20`
- `5_16`: `2`

Validation summary:

- Required PDFs: pass
- Parsed rows: pass
- Required forms present: pass
- Candidate master schema/scoped key checks: pass
- Candidate one-person-one-constituency check: pass
- Result candidate master match: warn, `10` unmatched constituency rows;
  `0` are still marked `ok`
- Result party master match: pass
- Thai text charset: warn, `10` rows; `0` are still marked `ok`
- `5_18` station coverage: pass, `341 / 341`
- `5_18` source evidence coverage: fail, `338 / 341`
- `5_18_partylist` station coverage: pass, `341 / 341`
- `5_18_partylist` source evidence coverage: fail, `318 / 341`
- Negative votes: pass
- Duplicate result rows: pass, `0`
- Vote totals over valid votes: pass
- Exact complete-group choice-vote totals: pass
- Ballot accounting: pass
- Needs review rows: warn, `18,915`

Accuracy report:

- Ground truth rows: `49 / 49`
- Overall field accuracy: `294 / 294 = 1.0`
- Row exact accuracy: `49 / 49 = 1.0`
- Important caveat: this is sample accuracy only. Do not claim full-district
  99% until more ground truth rows and P0 source/vote issues are reviewed.

Review queue:

- Total rows: `41,832`
- `P0`: `20,220`
- `P1`: `21,612`
- Top reasons:
  - `parser_marked_needs_review`: `18,915`
  - `missing_votes`: `18,888`
  - `master_data_unmatched`: `1,930`
  - `missing_source_page`: `1,332`
  - `low_ocr_confidence`: `757`
  - `invalid_text_charset`: `10`

P0 fallback targets:

- `5_18`: `568` targets, `1,572` affected rows
- `5_18_partylist`: `1,215` targets, `18,613` affected rows
- Missing source stations:
  - `5_18`: `339, 340, 341`
  - `5_18_partylist`:
    `9, 91, 124, 322, 323, 324, 325, 326, 327, 328, 329, 330,
    331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341`

Master data:

- `master_candidates.csv`: `7` rows
- `master_parties.csv`: `57` rows, `57` unique party numbers

## Known Problems

1. Parsed station coverage is now complete, but source evidence is not.
   - `5_18`: `338 / 341` stations have source PDF/page evidence
   - `5_18_partylist`: `318 / 341` stations have source PDF/page evidence
   - Rows without source evidence are scaffold rows and must stay
     `needs_review` until a real PDF/page and vote values are confirmed.

2. Many rows still need review.
   - Current `needs_review_rows`: `18,915`
   - Current P0 rows: `20,220`

3. Current sample accuracy is perfect but too small for a final claim.
   - `ground_truth_sample.csv` has `49` rows.
   - Add more reviewed units from different PDFs/subdistricts before claiming
     full 99% accuracy.

4. Some OCR/parser noise still creates unmatched candidate rows, but none of
   those rows are marked `ok`.
   - Current `result_candidate_master_matches`: warn, `10` rows unmatched.

5. For higher accuracy, do not OCR candidate/party names repeatedly.
   - Names should come from master files.
   - OCR should focus on `station_id`, numeric vote cells, and summary totals.
   - Recommended next OCR improvement is cell-crop digits OCR for P0/missing
     vote cells.

## Recommended Next Steps

1. Start with `data/processed/p0_fallback_targets.csv`.
   - Prioritize rows where `reasons` contains `missing_source_page`.
   - Then review `missing_votes` targets by source PDF/page.
2. Locate or confirm source pages for:
   - `5_18` stations `339,340,341`
   - `5_18_partylist` stations `9,91,124,322-341`
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
First, inspect `data/processed/p0_fallback_targets.csv` and fix the
`missing_source_page` rows by locating the real PDF/page or marking the source
as unavailable. Then use crop/Google/manual review to fill P0 `missing_votes`
into `data/external/reviewed_vote_cells.csv`, rerun `run_all --skip-ocr`, and
report source evidence coverage, review queue counts, and accuracy.
