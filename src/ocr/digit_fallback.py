from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.ocr.digit_crops import write_digit_crop_manifest
from src.ocr.digits import extract_digit_cell_value
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

VARIANT_PRIORITY = {"threshold3x": 0, "gray2x": 1, "raw": 2}


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
        return {
            **base,
            "status": "tesseract_unavailable",
            "notes": "Install tesseract or use Google Vision fallback for these crop paths.",
        }

    outputs = _ocr_crop_paths(ok_rows, tesseract_bin=tesseract_bin, psms=psms, lang=lang)
    values = [int(output["value"]) for output in outputs if output.get("value") is not None]
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
