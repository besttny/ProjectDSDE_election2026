# ProjectDSDE_election2026
# 🗳️ Data Science for Thailand Election 2026
**Course:** 2110446 Data Science and Data Engineering  
**Constituency:** ชัยภูมิ เขต 2 
**Polling Stations:** 341

## 📁 Project Structure
```
ProjectDSDE_election2026/
├── data/
│   ├── raw/
│   │   ├── pdfs/          # PDF ต้นฉบับจาก ECT
│   │   └── images/        # รูปภาพที่แปลงจาก PDF สำหรับ OCR/cache
│   ├── processed/         # ข้อมูลดิบหลัง OCR/parse เช่น checkpoint, raw extracted CSV/Parquet
│   ├── clean_data/        # ข้อมูลที่ clean/validate/impute แล้ว พร้อมใช้ EDA/dashboard
│   └── external/          # ข้อมูลอ้างอิง เช่น stations, candidates, parties, manifest
│
├── notebooks/
│   ├── 01_download_data.ipynb
│   ├── 02_ocr_extraction.ipynb
│   ├── 03_cleaning_validation.ipynb
│   ├── 04_eda_analysis.ipynb
│   ├── 05_comparison_analysis.ipynb
│   └── dashboard.py
│
├── run_batch.py           # CLI สำหรับ OCR/process/aggregate/QA
├── requirements.txt
└── README.md
```

## Setup

ใช้ Python virtual environment เพื่อให้ dependency ตรงกันระหว่างเครื่อง:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
```

OCR ต้องใช้ Poppler สำหรับ `pdf2image`:

```bash
brew install poppler
```

ถ้าจะยิง Typhoon OCR ให้สร้าง `.env` จากตัวอย่าง:

```bash
cp .env.example .env
# แล้วใส่ TYPHOON_API_KEY=...
```

อย่า commit `.env` เพราะมี API key

## Safe Run Order

1. เตรียม reference/source manifest:

```bash
jupyter notebook notebooks/01_download_data.ipynb
```

2. ทดสอบ OCR แบบ smoke test ก่อน:

```bash
.venv/bin/python run_batch.py run --limit-pdfs 1
```

3. ถ้าตรวจ cache/output แล้วค่อยรัน OCR เต็มชุด:

```bash
.venv/bin/python run_batch.py run
```

4. รวม checkpoint เป็นไฟล์ processed:

```bash
.venv/bin/python run_batch.py aggregate
.venv/bin/python run_batch.py qa
```

5. เปิด notebook cleaning/EDA/comparison ตามลำดับ:

```text
notebooks/03_cleaning_validation.ipynb
notebooks/04_eda_analysis.ipynb
notebooks/05_comparison_analysis.ipynb
```

6. เปิด dashboard:

```bash
.venv/bin/streamlit run notebooks/dashboard.py
```

## OCR Safety Notes

- `notebooks/02_ocr_extraction.ipynb` ตั้ง default เป็น smoke test 1 PDF
- ถ้าจะรันเต็มชุดใน notebook ต้องตั้ง `RUN_FULL_OCR = True` เอง
- `OCR_FORCE = False` เป็น default เพื่อไม่ reprocess cache โดยไม่ตั้งใจ
- การไม่มี `TYPHOON_API_KEY` จะ error เฉพาะตอน OCR request จริง ไม่ใช่ตอน import/setup

## Validation

รันชุดตรวจที่ไม่ยิง OCR และไม่สร้าง output ใหม่:

```bash
.venv/bin/python -m pytest
python3 -m py_compile run_batch.py notebooks/dashboard.py
```

ชุด test ปัจจุบันตรวจว่า Python scripts compile ได้, notebook JSON/code cells compile ได้, และ notebook OCR ไม่มี full-batch call แบบไม่ guarded

## Git Hygiene

โฟลเดอร์ `data/` ถูก ignore เพราะเป็น generated/local artifact ขนาดใหญ่ ถ้าต้องส่งข้อมูลตัวอย่างหรือไฟล์ review ให้ยืนยันก่อนว่าจะ track ไฟล์ใดเป็นพิเศษ
