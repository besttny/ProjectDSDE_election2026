# Data Science for Thailand Election 2026

**Course:** 2110446 Data Science and Data Engineering  
**Constituency:** ชัยภูมิ เขต 2
**Expected polling stations:** 341

This repository implements an end-to-end workflow for the course project:

`ECT PDF -> page images -> OCR -> parsed tables -> cleaning/corrections -> validation -> analysis -> Streamlit dashboard`

The current implementation is designed so the project can be reproduced after the official ECT PDFs are placed in `data/raw/pdfs/`.

## Project Structure

```text
configs/
  chaiyaphum_2.yaml              # Main project config
  chaiyaphum_2_manifest.csv      # Required ECT PDFs and expected coverage
data/
  raw/pdfs/                      # Official ECT PDFs, not committed
  raw/images/                    # Rendered PDF pages, generated
  raw/ocr/                       # Raw OCR JSON, generated
  external/manual_corrections.csv
  external/ground_truth_sample.csv
  external/master_*.csv
  processed/                     # Cleaned CSV/Parquet outputs
docs/
  architecture.md
notebooks/
  01_download_data.ipynb
  02_ocr_extraction.ipynb
  03_cleaning_validation.ipynb
  04_eda_analysis.ipynb
  05_dashboard_prep.ipynb
src/
  ocr/                           # PDF rendering, OCR engines, parser
  pipeline/                      # Manifest, cleaning, validation, run_all
  analysis/                      # Reports and figures
  dashboard/app.py               # Streamlit dashboard
tests/
```

## Setup

Use Python 3.11+.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

PaddleOCR and EasyOCR are intentionally listed in `requirements.txt` because Thai OCR is central to the project. They are large dependencies and may take several minutes to install.

## Raw Data

Download the official ECT PDFs for **ชัยภูมิ เขต 2** from:

https://www.ect.go.th/ect_th/th/election-2026

Place the files according to `configs/chaiyaphum_2_manifest.csv`:

```text
data/raw/pdfs/5_16/chaiyaphum_2_5_16.pdf
data/raw/pdfs/5_16_partylist/chaiyaphum_2_5_16_partylist.pdf
data/raw/pdfs/5_17/chaiyaphum_2_5_17.pdf
data/raw/pdfs/5_17_partylist/chaiyaphum_2_5_17_partylist.pdf
data/raw/pdfs/5_18/chaiyaphum_2_5_18.pdf
data/raw/pdfs/5_18_partylist/chaiyaphum_2_5_18_partylist.pdf
```

If the official filenames differ, either rename the files or update the `file_path` values in the manifest.

## Run the Pipeline

Run the full workflow:

```bash
python -m src.pipeline.run_all --config configs/chaiyaphum_2.yaml
```

For a quick OCR smoke test on only the first pages:

```bash
python -m src.ocr.extract --config configs/chaiyaphum_2.yaml --limit-pages 2
python -m src.pipeline.validate --config configs/chaiyaphum_2.yaml
```

For a resumable OCR batch, use the 1-based manifest row numbers. This is the
recommended mode for Google Colab or any low-RAM machine:

```bash
python -m src.pipeline.ocr_progress --config configs/chaiyaphum_2.yaml
python -m src.ocr.extract --config configs/chaiyaphum_2.yaml --start-index 5 --end-index 8
python -m src.pipeline.run_all --config configs/chaiyaphum_2.yaml --skip-ocr
```

Useful filters:

```bash
python -m src.ocr.extract --config configs/chaiyaphum_2.yaml --form-type 5_18_auto,5_18,5_18_partylist
python -m src.ocr.extract --config configs/chaiyaphum_2.yaml --file-contains "อำเภอเนินสง่า"
```

Rebuild cleaned outputs, validation, dashboard data, and reports without rerunning OCR:

```bash
python -m src.pipeline.run_all --config configs/chaiyaphum_2.yaml --skip-ocr
```

## Google Colab Workflow

Use Colab for the full OCR run if local memory is limited. The repo supports
batch OCR so Colab can resume after runtime resets without starting over.

1. Open `notebooks/02_ocr_extraction_colab.ipynb` in Colab.
2. Mount Google Drive and place the prepared PDF zip or extracted PDF folder in Drive.
3. Install dependencies from `requirements.txt`.
4. Run `python -m src.pipeline.ocr_progress --config configs/chaiyaphum_2.yaml`
   and choose a small manifest range, for example rows `5-8`.
5. Run OCR with `--start-index` and `--end-index`.
6. Zip `data/raw/ocr/` and `data/processed/parsed/` back to Drive after each batch.
7. Copy those folders back into this repo locally, then run
   `python -m src.pipeline.run_all --config configs/chaiyaphum_2.yaml --skip-ocr`.

Do not run the full 2,000+ page OCR locally unless the machine has enough RAM
and time. The local machine only needs the parsed/raw OCR artifacts to rebuild
validation, final CSVs, dashboard data, and reports.

## 99% Accuracy Workflow

Raw OCR accuracy is not enough for election data. Use these quality-control
outputs to make a defensible final dataset accuracy claim:

```bash
python -m src.quality.review_queue --config configs/chaiyaphum_2.yaml
python -m src.quality.master_match --config configs/chaiyaphum_2.yaml
python -m src.quality.evaluate_accuracy --config configs/chaiyaphum_2.yaml
```

Recommended process:

1. Fill `data/external/master_parties.csv`, `master_candidates.csv`, and
   `master_polling_stations.csv` with official names and aliases.
2. Run the review queue and fix every `P0` row first.
3. Use `data/processed/master_match_report.csv` to add repeatable aliases or
   manual corrections instead of silently editing final outputs.
4. For OCR rows that cannot be recovered reliably, enter the verified row in
   `data/external/reviewed_rows.csv`; this replaces OCR rows for the same
   source PDF page and form.
5. Manually review a sample of source PDF pages and enter the verified values
   into `data/external/ground_truth_sample.csv`.
6. Run the accuracy evaluator. Claim 99% only when
   `overall_field_accuracy` and `row_exact_accuracy` pass the configured
   `quality.target_accuracy` threshold.

## Dashboard

After running the pipeline:

```bash
streamlit run src/dashboard/app.py
```

The dashboard loads `data/processed/dashboard_dataset.parquet` when available and falls back to CSV.

## Main Outputs

- `data/processed/election_results_long.csv`
- `data/processed/polling_station_summary.csv`
- `data/processed/validation_report.csv`
- `data/processed/ocr_progress.csv`
- `data/processed/dashboard_dataset.parquet`
- `data/processed/constituency_votes.csv`
- `data/processed/partylist_votes.csv`
- `data/processed/review_queue.csv`
- `data/processed/master_match_report.csv`
- `data/processed/accuracy_report.csv`
- `data/processed/accuracy_details.csv`
- `outputs/reports/validation_report.md`
- `outputs/reports/accuracy_report.md`
- `outputs/reports/insights_report.md`
- `outputs/figures/top_choices.png`

## Validation Checks

The validator checks:

- all required PDF forms are present
- all six required forms are parsed
- `5_18` and `5_18_partylist` cover 341 polling stations
- votes are non-negative
- duplicate `form_type + polling_station_no + choice_no` rows are not present
- summed choice votes do not exceed extracted valid votes
- `valid_votes + invalid_votes + no_vote` matches `ballots_cast` when all fields exist
- low-confidence OCR rows are marked `needs_review`

## Manual Corrections

Use `data/external/manual_corrections.csv` for repeatable OCR cleanup.

Format:

```csv
match_column,match_value,target_column,new_value,reason
party_name,พรคข้อมูล,party_name,พรรคข้อมูล,OCR missing character
```

The correction step updates matching rows and preserves row count.

Use `data/external/reviewed_rows.csv` only after checking the source PDF page
manually. Reviewed rows are source-page scoped and replace OCR rows for the same
`source_pdf`, `source_page`, and `form_type`.

## Tests

```bash
pytest
```

The tests cover OCR table parsing, manual corrections, and validation failures.
