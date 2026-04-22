from pathlib import Path

import pandas as pd

from src.pipeline.reviewed_rows import apply_reviewed_rows, apply_reviewed_vote_cells


def test_apply_reviewed_rows_replaces_same_source_page_and_form(tmp_path: Path):
    parsed = pd.DataFrame(
        [
            {
                "province": "ชัยภูมิ",
                "form_type": "5_18",
                "source_pdf": "/repo/data/raw/pdfs/sample.pdf",
                "source_page": 1,
                "choice_no": 1,
                "votes": 3,
            },
            {
                "province": "ชัยภูมิ",
                "form_type": "5_18_partylist",
                "source_pdf": "/repo/data/raw/pdfs/sample.pdf",
                "source_page": 1,
                "choice_no": 1,
                "votes": 9,
            },
        ]
    )
    reviewed_path = tmp_path / "reviewed_rows.csv"
    reviewed_path.write_text(
        "province,form_type,source_pdf,source_page,choice_no,votes,validation_status\n"
        "ชัยภูมิ,5_18,data/raw/pdfs/sample.pdf,1,1,13,ok\n",
        encoding="utf-8",
    )

    output = apply_reviewed_rows(parsed, reviewed_path)

    assert len(output) == 2
    reviewed = output[output["form_type"] == "5_18"].iloc[0]
    partylist = output[output["form_type"] == "5_18_partylist"].iloc[0]
    assert reviewed["votes"] == 13
    assert partylist["votes"] == 9


def test_apply_reviewed_vote_cells_updates_only_matching_choice(tmp_path: Path):
    parsed = pd.DataFrame(
        [
            {
                "province": "ชัยภูมิ",
                "form_type": "5_18",
                "polling_station_no": 2,
                "source_pdf": "/repo/data/raw/pdfs/sample.pdf",
                "source_page": 7,
                "choice_no": 3,
                "votes": pd.NA,
                "ocr_engine": "paddleocr",
                "ocr_confidence": 0.4,
                "validation_status": "needs_review",
            },
            {
                "province": "ชัยภูมิ",
                "form_type": "5_18",
                "polling_station_no": 2,
                "source_pdf": "/repo/data/raw/pdfs/sample.pdf",
                "source_page": 7,
                "choice_no": 4,
                "votes": 8,
                "ocr_engine": "paddleocr",
                "ocr_confidence": 0.9,
                "validation_status": "ok",
            },
        ]
    )
    reviewed_path = tmp_path / "reviewed_vote_cells.csv"
    reviewed_path.write_text(
        "form_type,source_pdf,source_page,polling_station_no,choice_no,votes,ocr_engine,ocr_confidence,validation_status\n"
        "5_18,data/raw/pdfs/sample.pdf,7,2,3,156,google_digit_fallback,0.99,ok\n",
        encoding="utf-8",
    )

    output = apply_reviewed_vote_cells(parsed, reviewed_path)

    corrected = output[output["choice_no"] == 3].iloc[0]
    untouched = output[output["choice_no"] == 4].iloc[0]
    assert corrected["votes"] == 156
    assert corrected["ocr_engine"] == "google_digit_fallback"
    assert corrected["ocr_confidence"] == 0.99
    assert corrected["validation_status"] == "ok"
    assert untouched["votes"] == 8
    assert untouched["ocr_engine"] == "paddleocr"
