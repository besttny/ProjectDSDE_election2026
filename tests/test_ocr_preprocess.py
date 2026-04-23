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


def test_preprocess_image_for_ocr_can_remove_table_lines_without_erasing_digits(
    tmp_path: Path,
):
    image_path = tmp_path / "cell.png"
    image = Image.new("L", (80, 50), color=255)
    pixels = image.load()
    for x in range(80):
        pixels[x, 20] = 0
    for y in range(50):
        pixels[30, y] = 0
    for x in range(50, 54):
        for y in range(30, 36):
            pixels[x, y] = 0
    image.save(image_path)

    ocr_image = preprocess_image_for_ocr(
        image_path,
        output_dir=tmp_path / "ocr_inputs",
        profile_name="table_lines_removed",
        options={
            "mode": "table_lines_removed",
            "grayscale": True,
            "autocontrast": False,
            "line_removal": {
                "enabled": True,
                "threshold": 128,
                "thickness": 0,
                "horizontal_min_run_ratio": 0.8,
                "vertical_min_run_ratio": 0.8,
            },
        },
    )

    with Image.open(ocr_image.path) as cleaned:
        assert cleaned.getpixel((10, 20)) == 255
        assert cleaned.getpixel((30, 10)) == 255
        assert cleaned.getpixel((51, 32)) == 0
