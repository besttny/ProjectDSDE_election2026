from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.ocr.digit_crops import write_digit_crop_manifest
from src.ocr.digits import extract_digit_cell_value, extract_leading_digit_cell_value
from src.pipeline.config import ProjectConfig, load_config
from src.quality.master_keys import validate_choice_key

SUGGESTION_COLUMNS = [
    "row_index",
    "form_type",
    "source_pdf",
    "source_page",
    "polling_station_no",
    "choice_no",
    "choice_key_status",
    "selected_votes",
    "selected_variant",
    "selected_psm",
    "status",
    "ocr_outputs",
    "crop_paths",
    "notes",
]

VARIANT_PRIORITY = {"line_removed3x": 0, "threshold3x": 1, "gray2x": 2, "raw": 3}


def _read_manifest(config: ProjectConfig) -> pd.DataFrame:
    path = (
        config.output("p0_digit_crops_manifest")
        if "p0_digit_crops_manifest" in config.outputs
        else config.path("processed_dir") / "p0_digit_crops_manifest.csv"
    )
    if not path.exists():
        write_digit_crop_manifest(config)
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _group_key_columns(frame: pd.DataFrame) -> list[str]:
    return [
        column
        for column in [
            "row_index",
            "form_type",
            "source_pdf",
            "source_page",
            "polling_station_no",
            "choice_no",
        ]
        if column in frame.columns
    ]


def _first_value(rows: pd.DataFrame, column: str) -> object:
    if column not in rows.columns or rows.empty:
        return ""
    for value in rows[column]:
        if pd.notna(value) and str(value).strip() != "":
            return value
    return ""


def _choice_status(config: ProjectConfig, rows: pd.DataFrame) -> str:
    existing = str(_first_value(rows, "choice_key_status")).strip()
    if existing:
        return existing
    return validate_choice_key(
        config,
        form_type=_first_value(rows, "form_type"),
        choice_no=_first_value(rows, "choice_no"),
    )


def _parse_crop_box(value: object) -> tuple[float, float, float, float] | None:
    if pd.isna(value):
        return None
    parts = [part.strip() for part in str(value).split(",")]
    if len(parts) != 4:
        return None
    try:
        left, top, right, bottom = (float(part) for part in parts)
    except ValueError:
        return None
    if right <= left or bottom <= top:
        return None
    return left, top, right, bottom


def _bbox(line: dict[str, object]) -> tuple[float, float, float, float] | None:
    points = line.get("bbox") or []
    if not points:
        return None
    xs = [float(point[0]) for point in points]
    ys = [float(point[1]) for point in points]
    return min(xs), min(ys), max(xs), max(ys)


def _scaled_bbox(
    box: tuple[float, float, float, float],
    payload: dict[str, object],
    *,
    image_width: float,
    image_height: float,
) -> tuple[float, float, float, float]:
    page_width = float(payload.get("page_width") or image_width)
    page_height = float(payload.get("page_height") or image_height)
    if page_width <= 0 or page_height <= 0:
        return box
    x_scale = image_width / page_width
    y_scale = image_height / page_height
    left, top, right, bottom = box
    return left * x_scale, top * y_scale, right * x_scale, bottom * y_scale


def _box_overlaps(
    a: tuple[float, float, float, float],
    b: tuple[float, float, float, float],
) -> bool:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    return max(ax1, bx1) < min(ax2, bx2) and max(ay1, by1) < min(ay2, by2)


def _candidate_output_value(
    output: dict[str, object],
    *,
    min_confidence: float = 0.35,
    max_value: int = 999,
) -> int | None:
    value = output.get("value")
    if value is None:
        return None
    confidence = output.get("confidence", "")
    if confidence != "":
        try:
            if float(confidence) < min_confidence:
                return None
        except (TypeError, ValueError):
            pass
    numeric = int(value)
    if numeric > max_value:
        return None
    return numeric


def _raw_ocr_digit_outputs(crop_rows: pd.DataFrame) -> list[dict[str, object]]:
    """Read candidate vote values from the raw PaddleOCR JSON for this crop.

    This is intentionally used before Tesseract because the full-page PaddleOCR
    output often already contains the vote number, but the parser could not map
    it to the master-driven expected row.
    """

    if crop_rows.empty:
        return []
    raw_path_value = _first_value(crop_rows, "raw_ocr_path")
    crop_box = _parse_crop_box(_first_value(crop_rows, "crop_box"))
    if not raw_path_value or crop_box is None:
        return []

    raw_path = Path(str(raw_path_value))
    if not raw_path.exists():
        return []
    try:
        payload = json.loads(raw_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    image_width = float(payload.get("page_width") or 1)
    image_height = float(payload.get("page_height") or 1)
    image_path_value = _first_value(crop_rows, "image_path")
    if image_path_value and Path(str(image_path_value)).exists():
        try:
            from PIL import Image

            with Image.open(Path(str(image_path_value))) as image:
                image_width, image_height = image.size
        except OSError:
            pass

    left, top, right, bottom = crop_box
    expanded_crop = (left - 8, top - 18, right + 80, bottom + 18)
    outputs: list[dict[str, object]] = []
    seen: set[tuple[str, int | None]] = set()
    for line in payload.get("lines") or []:
        bounds = _bbox(line)
        if bounds is None:
            continue
        image_bounds = _scaled_bbox(
            bounds,
            payload,
            image_width=image_width,
            image_height=image_height,
        )
        if not _box_overlaps(image_bounds, expanded_crop):
            continue
        _, line_top, _, line_bottom = image_bounds
        line_center_y = (line_top + line_bottom) / 2
        if not (top - 12 <= line_center_y <= bottom + 12):
            continue
        text = str(line.get("text", ""))
        value = extract_leading_digit_cell_value(text, max_digits=4)
        key = (text, value)
        if key in seen:
            continue
        seen.add(key)
        outputs.append(
            {
                "variant": "raw_ocr",
                "psm": "",
                "text": text,
                "value": value,
                "crop_path": "",
                "confidence": line.get("confidence", ""),
            }
        )
    return outputs


def _run_tesseract_digits(
    crop_path: Path,
    *,
    tesseract_bin: str,
    psm: int,
    lang: str,
    timeout_seconds: int = 20,
) -> str:
    command = [
        tesseract_bin,
        str(crop_path),
        "stdout",
        "--psm",
        str(psm),
        "-l",
        lang,
        "-c",
        "tessedit_char_whitelist=0123456789",
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if completed.returncode != 0:
        return ""
    return completed.stdout.strip()


def _ocr_crop_paths(
    crop_rows: pd.DataFrame,
    *,
    tesseract_bin: str,
    psms: Iterable[int],
    lang: str,
) -> list[dict[str, object]]:
    usable = crop_rows[crop_rows["status"].astype(str).eq("ok")].copy()
    if usable.empty:
        return []
    usable["_variant_priority"] = (
        usable["crop_variant"].astype(str).map(VARIANT_PRIORITY).fillna(99).astype(int)
    )
    usable = usable.sort_values(["_variant_priority", "crop_variant"], kind="stable")

    outputs: list[dict[str, object]] = []
    for _, row in usable.iterrows():
        crop_path = Path(str(row.get("crop_path", "")))
        if not crop_path.exists():
            continue
        for psm in psms:
            text = _run_tesseract_digits(
                crop_path,
                tesseract_bin=tesseract_bin,
                psm=psm,
                lang=lang,
            )
            value = extract_digit_cell_value(text, max_digits=6)
            outputs.append(
                {
                    "variant": str(row.get("crop_variant", "")),
                    "psm": psm,
                    "text": text,
                    "value": value,
                    "crop_path": str(crop_path),
                }
            )
    return outputs


def _summarize_group(
    config: ProjectConfig,
    rows: pd.DataFrame,
    *,
    tesseract_bin: str | None,
    psms: Iterable[int],
    lang: str,
) -> dict[str, object]:
    base = {
        "row_index": _first_value(rows, "row_index"),
        "form_type": _first_value(rows, "form_type"),
        "source_pdf": _first_value(rows, "source_pdf"),
        "source_page": _first_value(rows, "source_page"),
        "polling_station_no": _first_value(rows, "polling_station_no"),
        "choice_no": _first_value(rows, "choice_no"),
        "choice_key_status": _choice_status(config, rows),
        "selected_votes": "",
        "selected_variant": "",
        "selected_psm": "",
        "ocr_outputs": "",
        "crop_paths": "|".join(
            sorted(
                {
                    str(value)
                    for value in rows.get("crop_path", pd.Series(dtype=object))
                    if pd.notna(value) and str(value).strip()
                }
            )
        ),
    }

    if base["choice_key_status"] == "invalid":
        return {
            **base,
            "status": "skipped_invalid_choice_no",
            "notes": "Choice number is not in official master; fix parser row alignment before reading vote cells.",
        }

    max_value = int(
        config.quality.get(
            "max_vote_cell_value",
            config.quality.get("auto_digit_fallback_max_votes", 999),
        )
    )

    ok_rows = rows[rows["status"].astype(str).eq("ok")] if "status" in rows.columns else rows
    if ok_rows.empty:
        statuses = sorted({str(value) for value in rows.get("status", []) if str(value)})
        return {
            **base,
            "status": "no_usable_crop",
            "notes": "No crop image is available for digit OCR. Manifest statuses: "
            + ", ".join(statuses),
        }

    if not tesseract_bin:
        outputs = _raw_ocr_digit_outputs(ok_rows)
        raw_values = [
            value
            for output in outputs
            if (value := _candidate_output_value(output, max_value=max_value)) is not None
        ]
        unique_raw_values = sorted(set(raw_values))
        if len(unique_raw_values) == 1:
            selected = next(output for output in outputs if output.get("value") == unique_raw_values[0])
            return {
                **base,
                "selected_votes": unique_raw_values[0],
                "selected_variant": selected.get("variant", ""),
                "selected_psm": selected.get("psm", ""),
                "status": "candidate_suggestion",
                "ocr_outputs": json.dumps(outputs, ensure_ascii=False),
                "notes": "Review raw OCR vote-cell value, then copy it to data/external/reviewed_vote_cells.csv if source evidence agrees.",
            }
        if len(unique_raw_values) > 1:
            return {
                **base,
                "status": "ocr_conflict",
                "ocr_outputs": json.dumps(outputs, ensure_ascii=False),
                "notes": "Raw OCR found multiple vote values in this crop; review manually or send this cell to Google fallback.",
            }
        return {
            **base,
            "status": "tesseract_unavailable",
            "ocr_outputs": json.dumps(outputs, ensure_ascii=False),
            "notes": "Install tesseract or use Google Vision fallback for these crop paths.",
        }

    outputs = _raw_ocr_digit_outputs(ok_rows)
    raw_values = [
        value
        for output in outputs
        if (value := _candidate_output_value(output, max_value=max_value)) is not None
    ]
    unique_raw_values = sorted(set(raw_values))
    if len(unique_raw_values) == 1:
        selected = next(output for output in outputs if output.get("value") == unique_raw_values[0])
        return {
            **base,
            "selected_votes": unique_raw_values[0],
            "selected_variant": selected.get("variant", ""),
            "selected_psm": selected.get("psm", ""),
            "status": "candidate_suggestion",
            "ocr_outputs": json.dumps(outputs, ensure_ascii=False),
            "notes": "Review raw OCR vote-cell value, then copy it to data/external/reviewed_vote_cells.csv if source evidence agrees.",
        }

    if len(unique_raw_values) > 1:
        return {
            **base,
            "status": "ocr_conflict",
            "ocr_outputs": json.dumps(outputs, ensure_ascii=False),
            "notes": "Raw OCR found multiple vote values in this crop; review manually or send this cell to Google fallback.",
        }

    outputs = outputs + _ocr_crop_paths(ok_rows, tesseract_bin=tesseract_bin, psms=psms, lang=lang)
    values = [
        value
        for output in outputs
        if (value := _candidate_output_value(output, max_value=max_value)) is not None
    ]
    output_json = json.dumps(outputs, ensure_ascii=False)
    if not values:
        return {
            **base,
            "status": "ocr_blank_or_noisy",
            "ocr_outputs": output_json,
            "notes": "Digit-only OCR did not return a clean number; review crop or send this cell to Google fallback.",
        }

    unique_values = sorted(set(values))
    if len(unique_values) > 1:
        return {
            **base,
            "status": "ocr_conflict",
            "ocr_outputs": output_json,
            "notes": "Multiple digit values were read from the crop variants; review manually before applying.",
        }

    selected = next(output for output in outputs if output.get("value") == unique_values[0])
    return {
        **base,
        "selected_votes": unique_values[0],
        "selected_variant": selected.get("variant", ""),
        "selected_psm": selected.get("psm", ""),
        "status": "candidate_suggestion",
        "ocr_outputs": output_json,
        "notes": "Review this value, then copy it to data/external/reviewed_vote_cells.csv if source evidence agrees.",
    }


def build_digit_crop_suggestions(
    config: ProjectConfig,
    *,
    tesseract_bin: str | None = None,
    psms: Iterable[int] = (7, 8, 13),
    lang: str = "eng",
) -> pd.DataFrame:
    manifest = _read_manifest(config)
    if manifest.empty:
        return pd.DataFrame(columns=SUGGESTION_COLUMNS)

    if tesseract_bin is None:
        tesseract_bin = shutil.which("tesseract")

    records: list[dict[str, object]] = []
    for _, group in manifest.groupby(_group_key_columns(manifest), dropna=False, sort=False):
        records.append(
            _summarize_group(
                config,
                group,
                tesseract_bin=tesseract_bin,
                psms=psms,
                lang=lang,
            )
        )
    return pd.DataFrame(records, columns=SUGGESTION_COLUMNS)


def write_digit_crop_suggestions(
    config: ProjectConfig,
    *,
    tesseract_bin: str | None = None,
    psms: Iterable[int] = (7, 8, 13),
    lang: str = "eng",
) -> Path:
    config.ensure_output_dirs()
    output_path = (
        config.output("digit_crop_ocr_suggestions")
        if "digit_crop_ocr_suggestions" in config.outputs
        else config.path("processed_dir") / "digit_crop_ocr_suggestions.csv"
    )
    build_digit_crop_suggestions(
        config,
        tesseract_bin=tesseract_bin,
        psms=psms,
        lang=lang,
    ).to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Run safe digit-only OCR on prepared vote-cell crops.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    parser.add_argument("--tesseract-bin", default=None)
    parser.add_argument("--lang", default="eng")
    parser.add_argument(
        "--psm",
        action="append",
        type=int,
        default=None,
        help="Tesseract page segmentation mode. Repeat to try multiple modes.",
    )
    args = parser.parse_args()
    print(
        write_digit_crop_suggestions(
            load_config(args.config),
            tesseract_bin=args.tesseract_bin,
            psms=tuple(args.psm or [7, 8, 13]),
            lang=args.lang,
        )
    )


if __name__ == "__main__":
    main()
