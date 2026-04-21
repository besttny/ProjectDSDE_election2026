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
