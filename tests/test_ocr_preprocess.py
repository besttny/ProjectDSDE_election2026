from pathlib import Path

from PIL import Image

from src.ocr.language_filter import filter_ocr_lines_by_language
from src.ocr.preprocess import OCRImage, preprocess_image_for_ocr, rescale_ocr_lines


def test_preprocess_image_for_ocr_scales_and_rescales_bboxes(tmp_path: Path):
    image_path = tmp_path / "page.png"
    Image.new("RGB", (20, 10), color="white").save(image_path)

    ocr_image = preprocess_image_for_ocr(
        image_path,
        output_dir=tmp_path / "ocr_inputs",
        profile_name="thai_text",
        options={
            "mode": "thai_text",
            "grayscale": True,
            "autocontrast": True,
            "upscale": 2,
        },
    )

    assert ocr_image.path.exists()
    assert ocr_image.x_scale == 2
    assert ocr_image.y_scale == 2

    lines = [{"text": "ชัยภูมิ", "bbox": [[10, 4], [20, 4], [20, 8], [10, 8]]}]
    scaled = rescale_ocr_lines(lines, ocr_image)

    assert scaled[0]["bbox"] == [[5.0, 2.0], [10.0, 2.0], [10.0, 4.0], [5.0, 4.0]]


def test_rescale_ocr_lines_leaves_raw_image_coordinates_unchanged():
    lines = [{"text": "12", "bbox": [[1, 2], [3, 4]]}]

    assert rescale_ocr_lines(lines, OCRImage(path=Path("x.png"))) == lines


def test_filter_ocr_lines_by_language_keeps_thai_and_digits_but_drops_latin_noise():
    lines = [
        {"text": "ชัยภูมิ"},
        {"text": "123"},
        {"text": "AONwm"},
        {"text": "พรรค 12"},
    ]

    kept, dropped = filter_ocr_lines_by_language(lines, mode="thai_numeric")

    assert [line["text"] for line in kept] == ["ชัยภูมิ", "123", "พรรค 12"]
    assert dropped == 1
