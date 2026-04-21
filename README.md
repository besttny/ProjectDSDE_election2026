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

Rebuild cleaned outputs, validation, dashboard data, and reports without rerunning OCR:

```bash
python -m src.pipeline.run_all --config configs/chaiyaphum_2.yaml --skip-ocr
```

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
- `data/processed/dashboard_dataset.parquet`
- `outputs/reports/validation_report.md`
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
- low-confidence OCR rows are marked `needs_review`

## Manual Corrections

Use `data/external/manual_corrections.csv` for repeatable OCR cleanup.

Format:

```csv
match_column,match_value,target_column,new_value,reason
party_name,พรคข้อมูล,party_name,พรรคข้อมูล,OCR missing character
```

The correction step updates matching rows and preserves row count.

## Tests

```bash
pytest
```

The tests cover OCR table parsing, manual corrections, and validation failures.
