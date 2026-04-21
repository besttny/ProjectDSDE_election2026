from src.ocr.parser import parse_ocr_payload, parse_choice_line


def test_parse_choice_line_with_party_and_votes():
    parsed = parse_choice_line("1 นายทดสอบ พรรคตัวอย่าง 1,234")

    assert parsed == {
        "choice_no": 1,
        "choice_name": "นายทดสอบ",
        "party_name": "พรรคตัวอย่าง",
        "votes": 1234,
    }


def test_parse_ocr_payload_extracts_metadata_and_rows():
    payload = {
        "source_pdf": "sample.pdf",
        "source_page": 3,
        "ocr_engine": "paddleocr",
        "ocr_confidence": 0.91,
        "lines": [
            {"text": "หน่วยเลือกตั้งที่ 12", "confidence": 0.95, "bbox": [[0, 0], [1, 0]]},
            {"text": "อำเภอ เมืองชัยภูมิ ตำบล ในเมือง", "confidence": 0.90, "bbox": [[0, 10], [1, 10]]},
            {"text": "ผู้มีสิทธิเลือกตั้ง 1,000", "confidence": 0.92, "bbox": [[0, 20], [1, 20]]},
            {"text": "ผู้มาใช้สิทธิ 800", "confidence": 0.92, "bbox": [[0, 30], [1, 30]]},
            {"text": "บัตรดี 760", "confidence": 0.92, "bbox": [[0, 40], [1, 40]]},
            {"text": "บัตรเสีย 20", "confidence": 0.92, "bbox": [[0, 50], [1, 50]]},
            {"text": "ไม่ประสงค์ลงคะแนน 20", "confidence": 0.92, "bbox": [[0, 60], [1, 60]]},
            {"text": "1 นายทดสอบ พรรคตัวอย่าง 123", "confidence": 0.89, "bbox": [[0, 70], [1, 70]]},
            {"text": "2 นางตัวอย่าง พรรคข้อมูล 45", "confidence": 0.88, "bbox": [[0, 80], [1, 80]]},
        ],
    }

    rows = parse_ocr_payload(
        payload,
        province="ชัยภูมิ",
        constituency_no=2,
        form_type="5_18",
        vote_type="constituency",
        confidence_threshold=0.65,
    )

    assert len(rows) == 2
    assert rows[0]["polling_station_no"] == 12
    assert rows[0]["eligible_voters"] == 1000
    assert rows[0]["valid_votes"] == 760
    assert rows[0]["choice_no"] == 1
    assert rows[0]["votes"] == 123
    assert rows[0]["validation_status"] == "ok"

