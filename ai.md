# AI Handoff Notes

Last updated: 2026-05-04

## Current Checkout

- Repository: `/Users/pornmongkol/Documents/ProjectDSDE_election2026`
- Current branch: `ocr`
- Git status before creating this file was clean.
- Important: the current `ocr` branch is not the full runnable pipeline checkout.

## Main Finding

The `ocr` branch currently tracks only a small scaffold:

- notebooks
- README
- empty or placeholder folders
- an empty `requirements.txt`

It does not include the runnable Python source, configs, or test files needed for the pipeline. On this branch:

- `.venv/bin/python -m pytest` collects `0` tests.
- `.venv/bin/python -m src.pipeline.run_all --config configs/chaiyaphum_2.yaml --skip-ocr` fails because `src.pipeline.run_all` is missing.

## Runnable Branch

The runnable implementation is on branch `dev`.

Verified without switching branches by archiving `dev` into a temp directory and running against the local OCR artifacts:

- `pytest`: `89 passed`
- `run_all --skip-ocr`: completed successfully

Useful command when actually on `dev`:

```bash
.venv/bin/python -m pytest
.venv/bin/python -m src.pipeline.run_all --config configs/chaiyaphum_2.yaml --skip-ocr
```

## Latest Local Data Status

From the latest rebuild using `dev` source and local artifacts:

- `election_results_long.csv`: `21,903` rows
- `constituency_votes.csv`: `2,408` rows
- `partylist_votes.csv`: `19,486` rows
- `review_queue.csv`: `40,122` rows
- `p0_fallback_targets.csv`: `2,312` rows

Validation highlights:

- `5_18_station_coverage`: pass, `341 / 341`
- `5_18_partylist_station_coverage`: pass, `341 / 341`
- `5_18_source_evidence_coverage`: fail, `339 / 341`
- `5_18_partylist_source_evidence_coverage`: fail, `323 / 341`
- `needs_review_rows`: warn, `18,245`

Review queue:

- `P0`: `19,460`
- `P1`: `20,662`
- Top reasons:
  - `parser_marked_needs_review`: `18,245`
  - `missing_votes`: `18,232`
  - `master_data_unmatched`: `1,993`
  - `missing_source_page`: `1,228`
  - `low_ocr_confidence`: `416`
  - `invalid_text_charset`: `8`

Aggregate validation:

- `discrepancy`: `70`
- `missing_actual`: `4`

Accuracy report:

- Current sample reports `1.0`, but the sample is only `84` rows from `5_18` constituency data.
- Do not claim full-project 99% accuracy from this sample alone.

## Local Artifacts

Current local artifacts are present on disk but mostly ignored by Git:

- Raw PDFs: `35` files
- Raw OCR files: `2,036` files
- Raw images: `175` files
- `data/` size: about `1.8G`

## Git / Generated Output Notes

- Generated outputs should stay out of commits unless explicitly requested.
- There is a stash named `codex stash 2026-05-04`.
- The inspected stash contains generated reports and debug images, not missing pipeline source code.

## Recommended Next Steps

1. Switch to or base future implementation work on `dev`, not the current `ocr` branch.
2. Fix provenance/source evidence failures:
   - `5_18`: stations `340`, `341`
   - `5_18_partylist`: especially stations without any evidence such as `118`, `133`, `326-341`
3. Reduce P0 review items, especially `missing_votes`.
4. Inspect aggregate validation discrepancies against source PDFs and crops.
5. Expand ground truth beyond the current 84-row `5_18` constituency sample before making accuracy claims.
