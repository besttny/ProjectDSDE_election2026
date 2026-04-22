import pandas as pd

from src.ocr.text_constraints import (
    apply_thai_text_constraints,
    has_invalid_thai_text_chars,
    invalid_thai_text_mask,
)


def test_thai_text_constraints_allow_thai_text_arabic_digits_and_light_punctuation():
    assert not has_invalid_thai_text_chars("ทต.จัตุรัส 1")
    assert not has_invalid_thai_text_chars("กุดน้ำใส-บ้านกอก")


def test_thai_text_constraints_reject_latin_ocr_noise():
    assert has_invalid_thai_text_chars("NOnWm")
    assert has_invalid_thai_text_chars("Aรนจก")


def test_invalid_thai_text_mask_checks_known_text_fields():
    rows = pd.DataFrame(
        [
            {"choice_name": "นาย ก", "party_name": "เพื่อไทย", "validation_status": "ok"},
            {"choice_name": "OCR noise", "party_name": "เพื่อไทย", "validation_status": "ok"},
        ]
    )

    mask = invalid_thai_text_mask(rows)

    assert mask.tolist() == [False, True]


def test_apply_thai_text_constraints_marks_invalid_text_needs_review():
    rows = pd.DataFrame(
        [
            {"choice_name": "นาย ก", "party_name": "เพื่อไทย", "validation_status": "ok"},
            {"choice_name": "Aรนจก", "party_name": "เพื่อไทย", "validation_status": "ok"},
        ]
    )

    constrained = apply_thai_text_constraints(rows)

    assert constrained.loc[0, "validation_status"] == "ok"
    assert constrained.loc[1, "validation_status"] == "needs_review"
