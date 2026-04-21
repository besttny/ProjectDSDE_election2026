from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from PIL import Image


@dataclass(frozen=True)
class OCRZone:
    name: str
    crop_box: tuple[int, int, int, int]
    source: str


def image_size(image_path: Path) -> tuple[int, int]:
    with Image.open(image_path) as image:
        return image.size


def _bbox(line: dict[str, Any]) -> tuple[float, float, float, float] | None:
    points = line.get("bbox") or []
    if not points:
        return None
    xs = [float(point[0]) for point in points]
    ys = [float(point[1]) for point in points]
    return min(xs), min(ys), max(xs), max(ys)


def _bbox_min_y(line: dict[str, Any]) -> float:
    bounds = _bbox(line)
    return bounds[1] if bounds else 0.0


def _line_has_any(line: dict[str, Any], keywords: tuple[str, ...]) -> bool:
    text = str(line.get("text", "")).strip()
    return any(keyword in text for keyword in keywords)


def _line_is_table_anchor(line: dict[str, Any]) -> bool:
    text = str(line.get("text", "")).strip()
    return (
        "จำนวนคะแนน" in text
        or "ผู้สมัครรับเลือกตั้งแต่ละคน" in text
        or "ได้คะแนน" in text
        or ("หมาย" in text and "ตัว" in text)
    )


def _clamp_box(
    *,
    width: int,
    height: int,
    left: float,
    top: float,
    right: float,
    bottom: float,
) -> tuple[int, int, int, int]:
    x1 = max(0, min(width - 1, int(round(left))))
    y1 = max(0, min(height - 1, int(round(top))))
    x2 = max(x1 + 1, min(width, int(round(right))))
    y2 = max(y1 + 1, min(height, int(round(bottom))))
    return x1, y1, x2, y2


def _first_anchor_y(
    lines: list[dict[str, Any]], predicate: Callable[[dict[str, Any]], bool]
) -> float | None:
    ys = [_bbox_min_y(line) for line in lines if predicate(line)]
    return min(ys) if ys else None


def detect_ocr_zones(
    image_path: Path,
    full_page_lines: list[dict[str, Any]],
    *,
    options: dict[str, Any] | None = None,
) -> list[OCRZone]:
    """Detect stable ECT form zones from full-page OCR anchors.

    The ECT forms are mostly fixed-layout PDFs. We use OCR text anchors when
    available and fall back to ratio boxes so the workflow remains automatic
    even when a single anchor is missed.
    """

    options = options or {}
    width, height = image_size(image_path)

    metadata_ys = [
        _bbox_min_y(line)
        for line in full_page_lines
        if _line_has_any(line, ("หน่วยเลือกตั้ง", "อำเภอ/เขต", "ตำบล/แขวง"))
    ]
    if metadata_ys:
        metadata_top = min(metadata_ys) - height * 0.035
        metadata_bottom = max(metadata_ys) + height * 0.07
        metadata_source = "anchor"
    else:
        metadata_top = height * 0.24
        metadata_bottom = height * 0.34
        metadata_source = "ratio_fallback"

    summary_start = _first_anchor_y(
        full_page_lines,
        lambda line: _line_has_any(
            line,
            (
                "จำนวนผู้มีสิทธิ",
                "ผู้มีสิทธิเลือกตั้ง",
                "จำนวนบัตรเลือกตั้ง",
                "บัตรดี",
            ),
        ),
    )
    table_start = _first_anchor_y(full_page_lines, _line_is_table_anchor)

    if summary_start is None:
        summary_top = height * 0.32
        summary_source = "ratio_fallback"
    else:
        summary_top = summary_start - height * 0.018
        summary_source = "anchor"
    summary_bottom = (table_start - height * 0.02) if table_start else height * 0.58
    summary_bottom = max(summary_bottom, summary_top + height * 0.12)

    if table_start is None:
        table_top = height * 0.55
        table_source = "ratio_fallback"
    else:
        table_top = table_start - height * 0.018
        table_source = "anchor"

    table_line_bottoms = [
        (_bbox(line) or (0.0, 0.0, 0.0, 0.0))[3]
        for line in full_page_lines
        if _bbox_min_y(line) >= table_top
    ]
    table_bottom = (
        max(table_line_bottoms) + height * 0.035
        if table_line_bottoms
        else height * 0.96
    )
    table_bottom = max(table_bottom, height * 0.92)

    boxes = [
        OCRZone(
            "metadata",
            _clamp_box(
                width=width,
                height=height,
                left=width * 0.07,
                top=metadata_top,
                right=width * 0.95,
                bottom=metadata_bottom,
            ),
            metadata_source,
        ),
        OCRZone(
            "summary",
            _clamp_box(
                width=width,
                height=height,
                left=width * 0.13,
                top=summary_top,
                right=width * 0.94,
                bottom=summary_bottom,
            ),
            summary_source,
        ),
        OCRZone(
            "table",
            _clamp_box(
                width=width,
                height=height,
                left=width * 0.08,
                top=table_top,
                right=width * 0.94,
                bottom=table_bottom,
            ),
            table_source,
        ),
    ]

    enabled_names = set(options.get("zones", ["metadata", "summary", "table"]))
    return [zone for zone in boxes if zone.name in enabled_names]


def crop_zone_image(image_path: Path, zone: OCRZone, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    crop_path = output_dir / f"{image_path.stem}__{zone.name}{image_path.suffix}"
    with Image.open(image_path) as image:
        image.crop(zone.crop_box).save(crop_path)
    return crop_path


def shift_lines_to_page(
    lines: list[dict[str, Any]],
    *,
    zone: OCRZone,
    ocr_engine: str,
) -> list[dict[str, Any]]:
    left, top, _, _ = zone.crop_box
    shifted: list[dict[str, Any]] = []
    for line in lines:
        copied = dict(line)
        copied["zone"] = zone.name
        copied["zone_source"] = zone.source
        copied["line_ocr_engine"] = ocr_engine
        shifted_bbox: list[list[float]] = []
        for point in copied.get("bbox") or []:
            shifted_bbox.append([float(point[0]) + left, float(point[1]) + top])
        copied["bbox"] = shifted_bbox
        shifted.append(copied)
    return shifted


def tag_full_page_lines(
    lines: list[dict[str, Any]],
    *,
    ocr_engine: str,
) -> list[dict[str, Any]]:
    tagged: list[dict[str, Any]] = []
    for line in lines:
        copied = dict(line)
        copied.setdefault("zone", "full_page")
        copied.setdefault("line_ocr_engine", ocr_engine)
        tagged.append(copied)
    return tagged
