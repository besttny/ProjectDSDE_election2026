from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class OCRDependencyError(RuntimeError):
    """Raised when a requested OCR backend is not installed."""


class OCREngine(Protocol):
    name: str

    def read(self, image_path: Path) -> list[dict[str, Any]]:
        ...


def _normalize_bbox(value: Any) -> list[list[float]]:
    if value is None:
        return []
    return [[float(point[0]), float(point[1])] for point in value]


class PaddleOCREngine:
    name = "paddleocr"

    def __init__(self, languages: list[str]) -> None:
        try:
            from paddleocr import PaddleOCR
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise OCRDependencyError(
                "paddleocr is not installed. Install requirements.txt or switch OCR engine."
            ) from exc

        lang = "th" if "th" in languages else languages[0]
        try:
            self._engine = PaddleOCR(use_angle_cls=True, lang=lang, show_log=False)
        except TypeError:
            self._engine = PaddleOCR(use_angle_cls=True, lang=lang)

    def read(self, image_path: Path) -> list[dict[str, Any]]:
        raw = self._engine.ocr(str(image_path), cls=True)
        return normalize_paddle_output(raw)


class EasyOCREngine:
    name = "easyocr"

    def __init__(self, languages: list[str]) -> None:
        try:
            import easyocr
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise OCRDependencyError(
                "easyocr is not installed. Install requirements.txt or switch OCR engine."
            ) from exc
        self._engine = easyocr.Reader(languages, gpu=False)

    def read(self, image_path: Path) -> list[dict[str, Any]]:
        raw = self._engine.readtext(str(image_path))
        return normalize_easyocr_output(raw)


def normalize_paddle_output(raw: Any) -> list[dict[str, Any]]:
    """Normalize PaddleOCR outputs across the older tuple and newer dict shapes."""

    lines: list[dict[str, Any]] = []
    if not raw:
        return lines

    first = raw[0] if isinstance(raw, list) else raw
    if isinstance(first, dict):
        texts = first.get("rec_texts", [])
        scores = first.get("rec_scores", [])
        boxes = first.get("rec_polys") or first.get("dt_polys") or []
        for text, score, box in zip(texts, scores, boxes, strict=False):
            lines.append(
                {
                    "text": str(text).strip(),
                    "confidence": float(score),
                    "bbox": _normalize_bbox(box),
                }
            )
        return lines

    pages = raw if isinstance(raw, list) else [raw]
    for page in pages:
        if not page:
            continue
        for item in page:
            if not item or len(item) < 2:
                continue
            bbox, text_info = item[0], item[1]
            if not text_info or len(text_info) < 2:
                continue
            text, score = text_info[0], text_info[1]
            lines.append(
                {
                    "text": str(text).strip(),
                    "confidence": float(score),
                    "bbox": _normalize_bbox(bbox),
                }
            )
    return lines


def normalize_easyocr_output(raw: Any) -> list[dict[str, Any]]:
    lines: list[dict[str, Any]] = []
    for item in raw or []:
        if len(item) < 3:
            continue
        bbox, text, score = item[0], item[1], item[2]
        lines.append(
            {
                "text": str(text).strip(),
                "confidence": float(score),
                "bbox": _normalize_bbox(bbox),
            }
        )
    return lines


def build_engine(name: str, languages: list[str]) -> OCREngine:
    normalized = name.strip().lower()
    if normalized == "paddleocr":
        return PaddleOCREngine(languages)
    if normalized == "easyocr":
        return EasyOCREngine(languages)
    raise ValueError(f"Unsupported OCR engine: {name}")


def average_confidence(lines: list[dict[str, Any]]) -> float:
    if not lines:
        return 0.0
    return sum(float(line.get("confidence", 0.0)) for line in lines) / len(lines)


def run_ocr_with_fallback(
    image_path: Path,
    *,
    primary_engine: OCREngine,
    fallback_engine: OCREngine | None,
    confidence_threshold: float,
) -> tuple[str, list[dict[str, Any]]]:
    primary_lines = primary_engine.read(image_path)
    if average_confidence(primary_lines) >= confidence_threshold or fallback_engine is None:
        return primary_engine.name, primary_lines

    fallback_lines = fallback_engine.read(image_path)
    if average_confidence(fallback_lines) > average_confidence(primary_lines):
        return fallback_engine.name, fallback_lines
    return primary_engine.name, primary_lines
