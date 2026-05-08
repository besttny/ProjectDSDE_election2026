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