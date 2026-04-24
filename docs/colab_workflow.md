# Google Colab OCR Workflow

This workflow keeps heavy OCR work off the local machine. Colab runs only the
PDF rendering and OCR batches; the local repo rebuilds cleaned datasets,
validation, analysis, and dashboard outputs from the returned artifacts.

## 1. Prepare Drive

Recommended Drive layout:

```text
MyDrive/election2026/
  ProjectDSDE_election2026/       # repo clone or uploaded repo folder
  raw_pdfs/                       # extracted ECT PDF folder or zip contents
  artifacts/                      # OCR batch zips exported from Colab
```

If the repo code is not pushed yet, upload a zip of the repo folder to Drive and
unzip it in Colab. If the code is pushed to GitHub, clone the `dev` branch.

For a ready-to-run full OCR workflow, open:

```text
notebooks/02_ocr_full_run_colab.ipynb
```

That notebook mounts Drive, installs a PaddleOCR-only environment, extracts
`raw_pdfs.zip`, runs manifest batches `1-35`, rebuilds reports, and exports the
artifact zip back to Drive.

For the current accuracy-first A100 run, keep these notebook defaults:

```python
USE_LOCAL_SCRATCH = True
START_FRESH = True
SPEED_PROFILE = "accuracy"
OVERWRITE = False
RUN_REPORTS_AFTER_EACH_BATCH = False
CHECKPOINT_EACH_BATCH = False
```

`accuracy` keeps the repo OCR defaults from `configs/chaiyaphum_2.yaml`, uses
`device=gpu:0`, `precision=fp32`, and full zone OCR (`metadata`, `summary`, and
`table`). The current config is accuracy-first: Thai-only PaddleOCR, high DPI,
form-specific table profiles, and conservative table-line removal. `enable_hpi`
is disabled by default because PaddleOCR needs the optional `ultra-infer`
package for that engine. Speed still comes from Colab local scratch storage and
building reports only once at the end. If the runtime is unstable, set
`CHECKPOINT_EACH_BATCH = True`; it is slower but writes `ocr_checkpoint_latest.zip`
for the next run.

## 2. Install Dependencies

In Colab:

```bash
pip install -r requirements.txt
```

Use a fresh runtime when PaddleOCR or EasyOCR dependency versions change.
For the full-run notebook, prefer its install cell instead of `requirements.txt`
because Colab can mix Paddle CUDA packages and EasyOCR/Torch CUDA packages.

## 3. Check Manifest Progress

```bash
python -m src.pipeline.ocr_progress --config configs/chaiyaphum_2.yaml
```

This writes:

```text
data/processed/ocr_progress.csv
```

Use `manifest_index` from that CSV as the batch index. Rows 1-4 are the advance
vote forms. Rows 5 onward are election-day PDFs.

## 4. Run OCR in Small Batches

Start with 3-5 manifest rows per Colab run:

```bash
python -m src.ocr.extract --config configs/chaiyaphum_2.yaml --start-index 5 --end-index 8
```

The OCR run is layout-aware. For each rendered page, the extractor first OCRs
the full page to locate anchors, then automatically crops and re-OCRs:

```text
metadata  -> station / district / subdistrict
summary   -> eligible voters, total ballots, valid, invalid, no-vote
table     -> candidate or party rows and vote counts
```

The raw JSON keeps the full-page lines plus shifted zone lines, so the parser
can use table OCR for vote rows and summary OCR for totals while still keeping
the original full-page OCR for audit.

Other useful filters:

```bash
python -m src.ocr.extract --config configs/chaiyaphum_2.yaml --form-type 5_18_auto,5_18,5_18_partylist
python -m src.ocr.extract --config configs/chaiyaphum_2.yaml --file-contains "อำเภอเนินสง่า"
```

The command is resumable. Existing raw OCR JSON files are reused unless
`--overwrite` is passed.

## 5. Rebuild Outputs Without OCR

After each batch:

```bash
python -m src.pipeline.run_all --config configs/chaiyaphum_2.yaml --skip-ocr
```

This updates validation, final CSV schema files, dashboard datasets, insights,
and OCR progress without rerunning OCR.

The row-level data must still come from OCR/source-page review of the official
`5/16`-`5/18` PDFs. If later you add `ส.ส. 6/1` or `6/1 (บช.)` references, use
them only to validate aggregate sums and flag discrepancies; do not use them to
overwrite OCR rows automatically.

Optional `6/1` references should be saved as
`data/external/aggregate_validation_reference.csv`; `run_all --skip-ocr` writes
`data/processed/aggregate_validation_report.csv` and flags discrepancies
without changing OCR-derived rows.

## 6. Export Colab Artifacts

Zip these folders after every batch:

```text
data/raw/ocr/
data/processed/parsed/
data/processed/ocr_progress.csv
data/processed/validation_report.csv
outputs/reports/
```

The rendered images under `data/raw/images/` are optional because they can be
regenerated. Zone crop images are also stored under that folder for audit, but
they do not need to be copied back if `data/raw/ocr/` and `data/processed/parsed/`
are exported. Do not commit large PDFs, rendered page images, OCR JSON, or
batch zip files.

## 7. Restore Locally

Unzip the Colab artifact into the repo root so the folder structure lines up:

```text
data/raw/ocr/...
data/processed/parsed/...
```

Then run locally:

```bash
python -m src.pipeline.ocr_progress --config configs/chaiyaphum_2.yaml
python -m src.pipeline.run_all --config configs/chaiyaphum_2.yaml --skip-ocr
streamlit run src/dashboard/app.py
```

Local verification does not need to load OCR models.
