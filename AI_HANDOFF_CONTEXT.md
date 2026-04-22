# AI Handoff Context - Election 2026 OCR Project

Last updated: 2026-04-22

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

- Total: `8,916`
- `5_18_partylist`: `7,064`
- `5_18`: `1,774`
- `5_17_partylist`: `29`
- `5_17`: `26`
- `5_16_partylist`: `20`
- `5_16`: `3`

Validation summary:

- Required PDFs: pass
- Parsed rows: pass
- Required forms present: pass
- Candidate master schema/scoped key checks: pass
- Candidate one-person-one-constituency check: pass
- `5_18` station coverage: fail, `329 / 341`
- `5_18_partylist` station coverage: fail, `315 / 341`
- Negative votes: pass
- Duplicate result rows: pass, `0`
- Vote totals over valid votes: pass
- Ballot accounting: pass
- Needs review rows: warn, `6,358`

Review queue:

- Total rows: `14,120`
- `P0`: `5,871`
- `P1`: `8,249`
- Top reasons:
  - `parser_marked_needs_review`: `6,358`
  - `missing_votes`: `5,871`
  - `master_data_unmatched`: `1,102`
  - `low_ocr_confidence`: `789`

Master data:

- `master_candidates.csv`: `7` rows
- `master_parties.csv`: `57` rows, `57` unique party numbers

## Known Problems

1. Coverage is not complete.
   - `5_18`: `329 / 341`
   - `5_18_partylist`: `315 / 341`

2. Many rows still need review.
   - Current `needs_review_rows`: `6,358`
   - Current P0 rows: `5,871`

3. Current `master_candidates.csv` may be incomplete.
   - It currently contains candidate numbers `1,2,3,4,5,6,8`.
   - Need verify official candidate list for `ชัยภูมิ เขต 2`.

4. Some OCR/parser noise can still create impossible candidate or party
   numbers. Add a result-master validation rule:
   - Constituency rows must match
     `province + constituency_no + choice_no` in `master_candidates.csv`.
   - Party-list rows must match `choice_no` in `master_parties.csv`.
   - Unmatched rows should be marked `needs_review` and preferably excluded
     from final export until reviewed.

5. For higher accuracy, do not OCR candidate/party names repeatedly.
   - Names should come from master files.
   - OCR should focus on `station_id`, numeric vote cells, and summary totals.
   - Recommended next OCR improvement is cell-crop digits OCR for P0/missing
     vote cells.

## Recommended Next Steps

1. Add result-master validation/cleaning rule so impossible candidate/party
   numbers cannot stay marked as `ok`.
2. Re-run:

```bash
python -m src.pipeline.run_all --config configs/chaiyaphum_2.yaml --skip-ocr
```

3. Review:

```bash
data/processed/validation_report.csv
data/processed/review_queue.csv
data/processed/constituency_votes.csv
data/processed/partylist_votes.csv
```

4. Fill or verify official `master_candidates.csv` for `ชัยภูมิ เขต 2`.
5. Implement cell-crop digits OCR/fallback for P0 missing vote cells.

## Suggested Prompt For New AI Chat

Read `AI_HANDOFF_CONTEXT.md` and continue from the recommended next steps.
First, add the result-master validation rule so impossible candidate/party
numbers cannot stay marked as `ok`, then run `run_all --skip-ocr` and report
the updated validation/review queue.

