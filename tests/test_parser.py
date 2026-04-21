from src.ocr.parser import (
    infer_form_and_vote_type,
    parse_choice_line,
    parse_choice_table,
    parse_ocr_payload,
)


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


def test_parse_choice_table_from_positioned_ocr_lines():
    lines = [
        {"text": "๒", "confidence": 0.9, "bbox": [[120, 1110], [140, 1110]]},
        {"text": "เพื่อชาติไทย", "confidence": 0.9, "bbox": [[370, 1091], [500, 1091]]},
        {"text": "15", "confidence": 0.8, "bbox": [[720, 1097], [760, 1097]]},
        {"text": "๓", "confidence": 0.9, "bbox": [[120, 1168], [140, 1168]]},
        {"text": "ใหม่", "confidence": 0.9, "bbox": [[370, 1151], [500, 1151]]},
        {"text": "...", "confidence": 0.6, "bbox": [[720, 1152], [760, 1152]]},
    ]

    rows = parse_choice_table(lines, vote_type="party_list")

    assert rows == [
        {"choice_no": 2, "choice_name": "", "party_name": "เพื่อชาติไทย", "votes": 15},
        {"choice_no": 3, "choice_name": "", "party_name": "ใหม่", "votes": None},
    ]


def test_infer_5_18_constituency_ignores_voter_list_phrase():
    texts = [
        "รายงานผลการนับคะแนนสมาชิกสภาผู้แทนราษฎรแบบแบ่งเขตเลือกตั้ง",
        "จำนวนผู้มีสิทธิเลือกตั้งตามบัญชีรายชื่อผู้มีสิทธิเลือกตั้ง",
    ]

    assert infer_form_and_vote_type(texts, form_type="5_18_auto", vote_type="auto") == (
        "5_18",
        "constituency",
    )


def test_parse_ocr_payload_prefers_table_zone_over_full_page_rows():
    payload = {
        "source_pdf": "sample.pdf",
        "source_page": 1,
        "ocr_engine": "paddleocr+zones",
        "ocr_confidence": 0.91,
        "page_width": 1000,
        "page_height": 1400,
        "lines": [
            {
                "text": "รายงานผลการนับคะแนนสมาชิกสภาผู้แทนราษฎรแบบแบ่งเขตเลือกตั้ง",
                "confidence": 0.95,
                "zone": "full_page",
                "bbox": [[260, 210], [740, 210], [740, 235], [260, 235]],
            },
            {
                "text": "หน่วยเลือกตั้งที่ 1",
                "confidence": 0.95,
                "zone": "metadata",
                "bbox": [[120, 360], [420, 360], [420, 390], [120, 390]],
            },
            {
                "text": "1 นายทดสอบ พรรคตัวอย่าง 3",
                "confidence": 0.80,
                "zone": "full_page",
                "bbox": [[120, 860], [760, 860], [760, 890], [120, 890]],
            },
            {
                "text": "1",
                "confidence": 0.95,
                "zone": "table",
                "bbox": [[120, 860], [140, 860], [140, 885], [120, 885]],
            },
            {
                "text": "นายทดสอบ",
                "confidence": 0.95,
                "zone": "table",
                "bbox": [[285, 860], [380, 860], [380, 885], [285, 885]],
            },
            {
                "text": "พรรคตัวอย่าง",
                "confidence": 0.95,
                "zone": "table",
                "bbox": [[480, 860], [580, 860], [580, 885], [480, 885]],
            },
            {
                "text": "13",
                "confidence": 0.95,
                "zone": "table",
                "bbox": [[700, 860], [730, 860], [730, 885], [700, 885]],
            },
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

    assert len(rows) == 1
    assert rows[0]["polling_station_no"] == 1
    assert rows[0]["choice_no"] == 1
    assert rows[0]["votes"] == 13
