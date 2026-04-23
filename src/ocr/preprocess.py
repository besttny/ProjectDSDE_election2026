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
