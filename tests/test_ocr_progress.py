from pathlib import Path

import fitz

from src.pipeline.config import ProjectConfig
from src.pipeline.manifest import ManifestEntry
from src.pipeline.ocr_progress import build_ocr_progress


def _config(root: Path) -> ProjectConfig:
    return ProjectConfig(
        root=root,
        data={
            "paths": {
                "raw_ocr_dir": "data/raw/ocr",
                "parsed_dir": "data/processed/parsed",
            }
        },
    )


def test_build_ocr_progress_marks_complete_when_raw_pages_and_parsed_rows_exist(tmp_path: Path):
    pdf_path = tmp_path / "sample.pdf"
    document = fitz.open()
    document.new_page()
    document.new_page()
    document.save(pdf_path)
    document.close()

    raw_dir = tmp_path / "data/raw/ocr/5_18/sample"
    raw_dir.mkdir(parents=True)
    (raw_dir / "sample_page_1.json").write_text("{}", encoding="utf-8")
    (raw_dir / "sample_page_2.json").write_text("{}", encoding="utf-8")

    parsed_dir = tmp_path / "data/processed/parsed"
    parsed_dir.mkdir(parents=True)
    (parsed_dir / "5_18_sample.csv").write_text("choice_no,votes\n1,10\n", encoding="utf-8")

    entry = ManifestEntry("5_18", "constituency", True, None, pdf_path, "", "")

    progress = build_ocr_progress(_config(tmp_path), [entry])

    assert progress.loc[0, "pdf_pages"] == 2
    assert progress.loc[0, "raw_json_pages"] == 2
    assert progress.loc[0, "parsed_rows"] == 1
    assert progress.loc[0, "status"] == "complete"


def test_build_ocr_progress_marks_missing_pdf(tmp_path: Path):
    entry = ManifestEntry("5_18", "constituency", True, None, tmp_path / "missing.pdf", "", "")

    progress = build_ocr_progress(_config(tmp_path), [entry])

    assert progress.loc[0, "status"] == "missing_pdf"
