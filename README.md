# ProjectDSDE_election2026
# 🗳️ Data Science for Thailand Election 2026
**Course:** 2110446 Data Science and Data Engineering  
**Constituency:** ชัยภูมิ เขต 2 
**Polling Stations:** 341

## 📁 Project Structure
```
election2026/
├── data/
│   ├── raw/
│   │   ├── pdfs/          # PDF ต้นฉบับจาก ECT
│   │   └── images/        # PDF แปลงเป็น image ก่อน OCR
│   ├── processed/         # ข้อมูลหลัง OCR + cleaning (CSV/Parquet)
│   └── external/          # ข้อมูลภายนอก
├── notebooks/
│   ├── 01_download_data.ipynb
│   ├── 02_ocr_extraction.ipynb
│   ├── 03_cleaning_validation.ipynb
│   ├── 04_eda_analysis.ipynb
│   └── 05_dashboard_prep.ipynb
├── src/
│   ├── ocr/               # OCR pipeline
│   ├── pipeline/          # Download & ETL scripts
│   ├── analysis/          # Analysis helpers
│   └── dashboard/         # Streamlit app
├── outputs/
│   ├── figures/
│   └── reports/
├── docs/
├── requirements.txt
└── README.md
```