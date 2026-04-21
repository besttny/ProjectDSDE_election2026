from PIL import Image

from src.ocr.zones import OCRZone, detect_ocr_zones, shift_lines_to_page


def test_detect_ocr_zones_uses_anchors(tmp_path):
    image_path = tmp_path / "page.png"
    Image.new("RGB", (1000, 1400), "white").save(image_path)
    lines = [
        {
            "text": "เลือกตั้งของหน่วยเลือกตั้งที่ 1",
            "bbox": [[120, 360], [420, 360], [420, 390], [120, 390]],
        },
        {
            "text": "๑.จำนวนผู้มีสิทธิเลือกตั้ง",
            "bbox": [[190, 460], [360, 460], [360, 485], [190, 485]],
        },
        {
            "text": "๓. จำนวนคะแนนที่ผู้สมัครรับเลือกตั้งแต่ละคนได้รับ",
            "bbox": [[190, 760], [790, 760], [790, 790], [190, 790]],
        },
    ]

    zones = {zone.name: zone for zone in detect_ocr_zones(image_path, lines)}

    assert set(zones) == {"metadata", "summary", "table"}
    assert zones["metadata"].source == "anchor"
    assert zones["summary"].crop_box[1] < 460
    assert zones["summary"].crop_box[3] < zones["table"].crop_box[1]
    assert zones["table"].crop_box[1] < 760


def test_shift_lines_to_page_preserves_full_page_coordinates():
    zone = OCRZone("table", (80, 740, 940, 1350), "anchor")
    shifted = shift_lines_to_page(
        [{"text": "13", "confidence": 0.9, "bbox": [[10, 20], [40, 20]]}],
        zone=zone,
        ocr_engine="paddleocr",
    )

    assert shifted[0]["zone"] == "table"
    assert shifted[0]["line_ocr_engine"] == "paddleocr"
    assert shifted[0]["bbox"] == [[90.0, 760.0], [120.0, 760.0]]
