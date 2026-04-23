from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from PIL import Image, ImageFilter, ImageOps


@dataclass(frozen=True)
class OCRImage:
    path: Path
    x_scale: float = 1.0
    y_scale: float = 1.0
    profile: str = "raw"


def _safe_profile_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")
    return cleaned or "ocr"


def _as_float(value: object, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: object, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _longest_dark_run(values: list[int], *, threshold: int) -> int:
    longest = 0
    current = 0
    for value in values:
        if value <= threshold:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _line_removal_options(options: dict[str, Any]) -> dict[str, Any]:
    line_removal = options.get("line_removal")
    if isinstance(line_removal, dict):
        merged = dict(line_removal)
        merged.setdefault("enabled", True)
        return merged
    if options.get("remove_lines"):
        return {"enabled": True}
    return {"enabled": False}


def _remove_ruled_lines(image: Image.Image, options: dict[str, Any]) -> Image.Image:
    line_options = _line_removal_options(options)
    if not line_options.get("enabled", False):
        return image

    cleaned = image.convert("L").copy()
    pixels = cleaned.load()
    width, height = cleaned.size
    threshold = _as_int(line_options.get("threshold", 175), 175)
    thickness = max(_as_int(line_options.get("thickness", 1), 1), 0)

    rows_to_clear: list[int] = []
    if line_options.get("horizontal", True):
        min_run = max(int(width * _as_float(line_options.get("horizontal_min_run_ratio", 0.45), 0.45)), 1)
        rows_to_clear = [
            y
            for y in range(height)
            if _longest_dark_run([pixels[x, y] for x in range(width)], threshold=threshold)
            >= min_run
        ]

    columns_to_clear: list[int] = []
    if line_options.get("vertical", True):
        min_run = max(int(height * _as_float(line_options.get("vertical_min_run_ratio", 0.55), 0.55)), 1)
        columns_to_clear = [
            x
            for x in range(width)
            if _longest_dark_run([pixels[x, y] for y in range(height)], threshold=threshold)
            >= min_run
        ]

    for y in rows_to_clear:
        for yy in range(max(0, y - thickness), min(height, y + thickness + 1)):
            for x in range(width):
                pixels[x, yy] = 255

    for x in columns_to_clear:
        for xx in range(max(0, x - thickness), min(width, x + thickness + 1)):
            for y in range(height):
                pixels[xx, y] = 255

    return cleaned


def preprocess_image_for_ocr(
    image_path: Path,
    *,
    output_dir: Path,
    profile_name: str,
    options: dict[str, Any] | None = None,
) -> OCRImage:
    options = options or {}
    mode = str(options.get("mode", "raw")).strip().lower()
    if not options.get("enabled", True) or mode in {"", "none", "raw"}:
        return OCRImage(path=image_path, profile="raw")

    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = image_path.suffix or ".png"
    output_path = output_dir / f"{image_path.stem}__ocr_{_safe_profile_name(profile_name)}{suffix}"

    with Image.open(image_path) as original:
        image = original.convert("L") if options.get("grayscale", True) else original.copy()
        if options.get("autocontrast", True):
            image = ImageOps.autocontrast(image)
        image = _remove_ruled_lines(image, options)
        if options.get("sharpen", False):
            image = image.filter(
                ImageFilter.UnsharpMask(
                    radius=_as_float(options.get("sharpen_radius", 1.0), 1.0),
                    percent=int(_as_float(options.get("sharpen_percent", 160), 160)),
                    threshold=int(_as_float(options.get("sharpen_threshold", 3), 3)),
                )
            )

        upscale = max(_as_float(options.get("upscale", 1.0), 1.0), 1.0)
        if upscale > 1.0:
            image = image.resize(
                (int(round(image.width * upscale)), int(round(image.height * upscale))),
                Image.Resampling.LANCZOS,
            )

        threshold = options.get("threshold")
        if threshold is not None:
            cutoff = int(_as_float(threshold, 180))
            image = image.point(lambda pixel: 255 if pixel > cutoff else 0)

        image.save(output_path)
        return OCRImage(
            path=output_path,
            x_scale=image.width / max(original.width, 1),
            y_scale=image.height / max(original.height, 1),
            profile=profile_name,
        )


def rescale_ocr_lines(lines: list[dict[str, Any]], ocr_image: OCRImage) -> list[dict[str, Any]]:
    if ocr_image.x_scale == 1.0 and ocr_image.y_scale == 1.0:
        return lines

    scaled: list[dict[str, Any]] = []
    for line in lines:
        copied = dict(line)
        copied["bbox"] = [
            [float(point[0]) / ocr_image.x_scale, float(point[1]) / ocr_image.y_scale]
            for point in copied.get("bbox") or []
        ]
        scaled.append(copied)
    return scaled
