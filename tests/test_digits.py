from src.ocr.digits import (
    extract_digit_cell_value,
    extract_first_int,
    extract_leading_digit_cell_value,
    normalize_digit_like_text,
)


def test_extract_digit_cell_value_accepts_thai_digits_and_common_ocr_confusions():
    assert extract_digit_cell_value("๑๒๓") == 123
    assert extract_digit_cell_value("O8") == 8
    assert extract_digit_cell_value("S1") == 51
    assert normalize_digit_like_text("๑,O๘") == "108"


def test_extract_digit_cell_value_rejects_text_noise():
    assert extract_digit_cell_value("พรรค 123") is None
    assert extract_digit_cell_value("นาย ก 13") is None
    assert extract_first_int("นาย ก 13") == 13


def test_extract_leading_digit_cell_value_handles_vote_cell_text_noise():
    assert extract_leading_digit_cell_value("1.0น") == 10
    assert extract_leading_digit_cell_value("54.ห") == 54
    assert extract_leading_digit_cell_value("108Mน01") == 108
    assert extract_leading_digit_cell_value("๔✓") == 4
    assert extract_leading_digit_cell_value(".0.80") is None
    assert extract_leading_digit_cell_value("SS7S") is None
