from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd
from PIL import Image, ImageOps

from src.ocr.digits import extract_digit_cell_value
from src.pipeline.config import ProjectConfig, load_config
from src.quality.master_keys import validate_choice_key
from src.quality.review_queue import write_review_queue

MANIFEST_COLUMNS = [
    "row_index",
    "priority",
    "reason",
    "source_pdf",
    "source_page",
    "form_type",
    "polling_station_no",
    "choice_no",
    "image_path",
    "raw_ocr_path",
    "crop_variant",
    "crop_path",
    "crop_box",
    "choice_key_status",
    "status",
    "notes",
]

FORM_PRIORITY = {
    "5_18": 0,
    "5_18_partylist": 1,
    "5_16": 2,
    "5_16_partylist": 3,
    "5_17": 4,
    "5_17_partylist": 5,
}


def _safe_part(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    text = re.sub(r"[^\w.\-\u0E00-\u0E7F]+", "_", text, flags=re.UNICODE).strip("_")
    return text[:120] or "unknown"


def _source_stem(source_pdf: object) -> str:
    return Path(str(source_pdf)).stem


def _page_no(value: object) -> int | None:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return int(numeric)


def _read_review_queue(config: ProjectConfig) -> pd.DataFrame:
    path = config.output("review_queue")
    if not path.exists():
        write_review_queue(config)
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _read_row_index_filter(path: Path | None) -> set[int] | None:
    if path is None:
        return None
    if not path.exists():
        raise FileNotFoundError(f"Row-index filter file not found: {path}")
    frame = pd.read_csv(path)
    if "row_index" not in frame.columns:
        raise ValueError("Row-index filter CSV must include a row_index column")
    values = pd.to_numeric(frame["row_index"], errors="coerce").dropna().astype(int)
    return set(values.tolist())


read_row_index_filter = _read_row_index_filter


def _candidate_paths(base_dir: Path, source_pdf: object, page_no: int) -> list[Path]:
    stem = _source_stem(source_pdf)
    pattern = f"*/{stem}/{stem}_page_{page_no:04d}.*"
    return sorted(path for path in base_dir.glob(pattern) if path.is_file())


def _find_page_image(config: ProjectConfig, source_pdf: object, page_no: int) -> Path | None:
    for path in _candidate_paths(config.path("raw_image_dir"), source_pdf, page_no):
        if path.suffix.lower() in {".png", ".jpg", ".jpeg"} and "/zones/" not in str(path):
            return path
    return None


def _local_source_pdf_path(config: ProjectConfig, source_pdf: object) -> Path | None:
    source_path = Path(str(source_pdf))
    if source_path.exists():
        return source_path

    marker = "data/raw/pdfs/"
    source_text = str(source_pdf)
    if marker in source_text:
        relative_pdf = source_text.split(marker, 1)[1]
        candidate = config.path("raw_pdf_dir") / relative_pdf
        if candidate.exists():
            return candidate

    for candidate in sorted(config.path("raw_pdf_dir").rglob(source_path.name)):
        if candidate.is_file():
            return candidate
    return None


def _render_page_image(
    config: ProjectConfig,
    *,
    source_pdf: object,
    form_type: object,
    page_no: int,
) -> Path | None:
    pdf_path = _local_source_pdf_path(config, source_pdf)
    if pdf_path is None:
        return None

    image_format = str(config.ocr.get("image_format", "png")).lower()
    output_dir = config.path("raw_image_dir") / _safe_part(form_type) / pdf_path.stem
    image_path = output_dir / f"{pdf_path.stem}_page_{page_no:04d}.{image_format}"
    if image_path.exists():
        return image_path

    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        import fitz
    except ImportError:
        pdftoppm = shutil.which("pdftoppm")
        if pdftoppm is None:
            return None
        output_prefix = output_dir / f"{pdf_path.stem}_page_{page_no:04d}"
        command = [
            pdftoppm,
            "-r",
            str(int(config.ocr.get("dpi", 300))),
            "-f",
            str(page_no),
            "-l",
            str(page_no),
            "-singlefile",
            "-png" if image_format == "png" else "-jpeg",
            str(pdf_path),
            str(output_prefix),
        ]
        subprocess.run(command, check=True, capture_output=True)
        return image_path if image_path.exists() else None

    dpi = int(config.ocr.get("dpi", 300))
    matrix = fitz.Matrix(dpi / 72, dpi / 72)
    with fitz.open(pdf_path) as document:
        if page_no < 1 or page_no > document.page_count:
            return None
        page = document.load_page(page_no - 1)
        pixmap = page.get_pixmap(matrix=matrix, alpha=False)
        pixmap.save(image_path)
    return image_path


def _find_raw_ocr(config: ProjectConfig, source_pdf: object, page_no: int) -> Path | None:
    stem = _source_stem(source_pdf)
    raw_path = config.path("raw_ocr_dir").glob(f"*/{stem}/{stem}_page_{page_no:04d}.json")
    return next((path for path in sorted(raw_path) if path.is_file()), None)


def _bbox(line: dict[str, Any]) -> tuple[float, float, float, float] | None:
    points = line.get("bbox") or []
    if not points:
        return None
    xs = [float(point[0]) for point in points]
    ys = [float(point[1]) for point in points]
    return min(xs), min(ys), max(xs), max(ys)


def _table_box(payload: dict[str, Any]) -> tuple[float, float, float, float]:
    width = float(payload.get("page_width") or 1)
    height = float(payload.get("page_height") or 1)
    for zone in payload.get("zones") or []:
        if zone.get("name") == "table" and len(zone.get("crop_box") or []) == 4:
            left, top, right, bottom = zone["crop_box"]
            return float(left), float(top), float(right), float(bottom)
    return width * 0.08, height * 0.55, width * 0.94, height * 0.96


def _line_in_box(line: dict[str, Any], box: tuple[float, float, float, float]) -> bool:
    bounds = _bbox(line)
    if bounds is None:
        return False
    left, top, right, bottom = box
    x1, y1, x2, y2 = bounds
    cx = (x1 + x2) / 2
    cy = (y1 + y2) / 2
    return left <= cx <= right and top <= cy <= bottom


def _anchor_for_choice(
    payload: dict[str, Any],
    *,
    choice_no: int,
) -> tuple[float, float, float, float] | None:
    table_box = _table_box(payload)
    table_left, _, table_right, _ = table_box
    table_width = max(table_right - table_left, 1.0)
    anchors: list[tuple[int, tuple[float, float, float, float]]] = []
    for line in payload.get("lines") or []:
        bounds = _bbox(line)
        if bounds is None or not _line_in_box(line, table_box):
            continue
        x1, _, x2, _ = bounds
        if ((x1 + x2) / 2) > table_left + table_width * 0.24:
            continue
        number = extract_digit_cell_value(str(line.get("text", "")), max_digits=3)
        if number == choice_no:
            priority = 0 if str(line.get("zone", "")) == "table" else 1
            anchors.append((priority, bounds))
    if not anchors:
        return None
    anchors.sort(key=lambda item: (item[0], item[1][1], item[1][0]))
    return anchors[0][1]


def _clamp_box(
    box: tuple[float, float, float, float],
    *,
    width: int,
    height: int,
) -> tuple[int, int, int, int]:
    left, top, right, bottom = box
    x1 = max(0, min(width - 1, int(round(left))))
    y1 = max(0, min(height - 1, int(round(top))))
    x2 = max(x1 + 1, min(width, int(round(right))))
    y2 = max(y1 + 1, min(height, int(round(bottom))))
    return x1, y1, x2, y2


def _scale_payload_box_to_image(
    box: tuple[float, float, float, float],
    payload: dict[str, Any],
    *,
    image_width: int,
    image_height: int,
) -> tuple[float, float, float, float]:
    page_width = float(payload.get("page_width") or image_width)
    page_height = float(payload.get("page_height") or image_height)
    if page_width <= 0 or page_height <= 0:
        return box
    x_scale = image_width / page_width
    y_scale = image_height / page_height
    left, top, right, bottom = box
    return left * x_scale, top * y_scale, right * x_scale, bottom * y_scale


def _crop_box_is_usable(
    crop_box: tuple[int, int, int, int],
    *,
    image_width: int,
    image_height: int,
) -> bool:
    x1, y1, x2, y2 = crop_box
    crop_width = x2 - x1
    crop_height = y2 - y1
    min_width = max(40, int(image_width * 0.018))
    min_height = max(18, int(image_height * 0.005))
    return crop_width >= min_width and crop_height >= min_height


def vote_cell_crop_box(
    payload: dict[str, Any],
    *,
    choice_no: int,
    image_width: int,
    image_height: int,
    template_slot: int | None = None,
    prefer_template: bool = False,
) -> tuple[int, int, int, int] | None:
    if prefer_template and template_slot is not None:
        template_box = template_vote_cell_crop_box(
            payload,
            row_slot=template_slot,
            image_width=image_width,
            image_height=image_height,
        )
        if template_box is not None:
            return template_box

    anchor = _anchor_for_choice(payload, choice_no=choice_no)
    if anchor is None:
        if template_slot is None:
            return None
        return template_vote_cell_crop_box(
            payload,
            row_slot=template_slot,
            image_width=image_width,
            image_height=image_height,
        )
    table_left, _, table_right, _ = _table_box(payload)
    table_width = max(table_right - table_left, 1.0)
    _, y1, _, y2 = anchor
    page_height = float(payload.get("page_height") or image_height)
    center_y = (y1 + y2) / 2
    line_height = max(y2 - y1, page_height * 0.012)
    crop_height = max(line_height * 2.4, page_height * 0.026)
    payload_crop_box = (
        table_left + table_width * 0.60,
        center_y - crop_height / 2,
        table_left + table_width * 0.76,
        center_y + crop_height / 2,
    )
    image_crop_box = _scale_payload_box_to_image(
        payload_crop_box,
        payload,
        image_width=image_width,
        image_height=image_height,
    )
    crop_box = _clamp_box(
        image_crop_box,
        width=image_width,
        height=image_height,
    )
    if not _crop_box_is_usable(
        crop_box,
        image_width=image_width,
        image_height=image_height,
    ):
        return None
    return crop_box


def template_vote_cell_crop_box(
    payload: dict[str, Any],
    *,
    row_slot: int,
    image_width: int,
    image_height: int,
) -> tuple[int, int, int, int] | None:
    """Fallback crop for fixed-layout 5/18 constituency candidate rows.

    Candidate number 7 is not present in Chaiyaphum constituency 2, but the
    printed table still keeps its vertical slot. Therefore the candidate number
    itself is the safest row slot for this form.
    """

    if row_slot <= 0 or row_slot > 12:
        return None
    page_width = float(payload.get("page_width") or image_width)
    page_height = float(payload.get("page_height") or image_height)
    center_y = page_height * 0.6215 + (row_slot - 1) * page_height * 0.0246
    crop_height = max(page_height * 0.023, 64.0)
    payload_crop_box = (
        page_width * 0.585,
        center_y - crop_height / 2,
        page_width * 0.665,
        center_y + crop_height / 2,
    )
    image_crop_box = _scale_payload_box_to_image(
        payload_crop_box,
        payload,
        image_width=image_width,
        image_height=image_height,
    )
    crop_box = _clamp_box(
        image_crop_box,
        width=image_width,
        height=image_height,
    )
    if not _crop_box_is_usable(
        crop_box,
        image_width=image_width,
        image_height=image_height,
    ):
        return None
    return crop_box


def partylist_template_vote_cell_crop_box(
    payload: dict[str, Any],
    *,
    choice_no: int,
    image_width: int,
    image_height: int,
) -> tuple[int, int, int, int] | None:
    """Fixed-layout vote-cell crop for 5/18 party-list rows.

    Party-list OCR often misses or misreads the printed party-list number,
    which makes anchor-based row detection unreliable. The official form uses
    stable continuation pages: choices 1-10 on the first table page, 11-34 on
    the second, and 35-57 on the third.
    """

    if choice_no <= 0 or choice_no > 57:
        return None

    page_width = float(payload.get("page_width") or image_width)
    page_height = float(payload.get("page_height") or image_height)
    if 1 <= choice_no <= 10:
        row_slot = choice_no
        center_y = page_height * 0.632 + (row_slot - 1) * page_height * 0.0299
    elif 11 <= choice_no <= 34:
        row_slot = choice_no - 10
        center_y = page_height * 0.313 + (row_slot - 1) * page_height * 0.0263
    else:
        row_slot = choice_no - 34
        center_y = page_height * 0.286 + (row_slot - 1) * page_height * 0.0264

    crop_height = max(page_height * 0.023, 64.0)
    payload_crop_box = (
        page_width * 0.50,
        center_y - crop_height / 2,
        page_width * 0.635,
        center_y + crop_height / 2,
    )
    image_crop_box = _scale_payload_box_to_image(
        payload_crop_box,
        payload,
        image_width=image_width,
        image_height=image_height,
    )
    crop_box = _clamp_box(
        image_crop_box,
        width=image_width,
        height=image_height,
    )
    if not _crop_box_is_usable(
        crop_box,
        image_width=image_width,
        image_height=image_height,
    ):
        return None
    return crop_box


def _save_crop_variants(
    image_path: Path,
    crop_box: tuple[int, int, int, int],
    output_base: Path,
) -> list[tuple[str, Path]]:
    output_base.parent.mkdir(parents=True, exist_ok=True)
    variants: list[tuple[str, Path]] = []
    with Image.open(image_path) as image:
        crop = image.crop(crop_box)
        raw_path = output_base.with_name(f"{output_base.name}__raw.png")
        crop.save(raw_path)
        variants.append(("raw", raw_path))

        gray = ImageOps.autocontrast(crop.convert("L"))
        gray = gray.resize((gray.width * 2, gray.height * 2), Image.Resampling.LANCZOS)
        gray_path = output_base.with_name(f"{output_base.name}__gray2x.png")
        gray.save(gray_path)
        variants.append(("gray2x", gray_path))

        threshold = ImageOps.autocontrast(crop.convert("L"))
        threshold = threshold.resize(
            (threshold.width * 3, threshold.height * 3),
            Image.Resampling.LANCZOS,
        )
        threshold = threshold.point(lambda pixel: 255 if pixel > 180 else 0)
        threshold_path = output_base.with_name(f"{output_base.name}__threshold3x.png")
        threshold.save(threshold_path)
        variants.append(("threshold3x", threshold_path))
    return variants


def _target_rows(
    queue: pd.DataFrame,
    *,
    max_targets: int | None,
    row_indexes: set[int] | None = None,
) -> pd.DataFrame:
    if queue.empty:
        return queue
    rows = queue[
        queue["priority"].astype(str).eq("P0")
        & queue["reason"].astype(str).eq("missing_votes")
        & queue["choice_no"].notna()
        & queue["source_page"].notna()
    ].copy()
    if row_indexes is not None:
        queue_row_indexes = pd.to_numeric(rows["row_index"], errors="coerce")
        rows = rows[queue_row_indexes.isin(row_indexes)].copy()
    rows = rows.drop_duplicates(subset=["row_index", "source_pdf", "source_page", "choice_no"])
    rows["_form_priority"] = (
        rows["form_type"].astype(str).map(FORM_PRIORITY).fillna(99).astype(int)
    )
    rows = rows.sort_values(
        ["_form_priority", "source_pdf", "source_page", "row_index"],
        kind="stable",
    ).drop(columns=["_form_priority"])
    if max_targets is not None:
        rows = rows.head(max_targets)
    return rows


def build_digit_crop_manifest(
    config: ProjectConfig,
    *,
    max_targets: int | None = None,
    row_indexes: set[int] | None = None,
) -> pd.DataFrame:
    queue = _read_review_queue(config)
    targets = _target_rows(queue, max_targets=max_targets, row_indexes=row_indexes)
    if targets.empty:
        return pd.DataFrame(columns=MANIFEST_COLUMNS)

    output_dir = config.path("raw_image_dir").parent / "crops" / "p0_digit_cells"
    rows: list[dict[str, object]] = []
    for _, target in targets.iterrows():
        source_page = _page_no(target.get("source_page"))
        choice_no = _page_no(target.get("choice_no"))
        if source_page is None or choice_no is None:
            continue
        image_path = _find_page_image(config, target.get("source_pdf"), source_page)
        if image_path is None:
            image_path = _render_page_image(
                config,
                source_pdf=target.get("source_pdf"),
                form_type=target.get("form_type", ""),
                page_no=source_page,
            )
        raw_path = _find_raw_ocr(config, target.get("source_pdf"), source_page)
        base_record = {
            "row_index": target.get("row_index", ""),
            "priority": target.get("priority", ""),
            "reason": target.get("reason", ""),
            "source_pdf": target.get("source_pdf", ""),
            "source_page": source_page,
            "form_type": target.get("form_type", ""),
            "polling_station_no": target.get("polling_station_no", ""),
            "choice_no": choice_no,
            "image_path": str(image_path or ""),
            "raw_ocr_path": str(raw_path or ""),
        }
        choice_key_status = validate_choice_key(
            config,
            form_type=target.get("form_type", ""),
            choice_no=choice_no,
            province=target.get("province", ""),
            constituency_no=target.get("constituency_no", ""),
        )
        base_record["choice_key_status"] = choice_key_status
        if choice_key_status == "invalid":
            rows.append(
                {
                    **base_record,
                    "crop_variant": "",
                    "crop_path": "",
                    "crop_box": "",
                    "status": "invalid_choice_no",
                    "notes": (
                        "Choice number is not present in the official master for this "
                        "form/province/constituency. Review parser row alignment before "
                        "digit OCR."
                    ),
                }
            )
            continue
        if image_path is None:
            rows.append({**base_record, "crop_variant": "", "crop_path": "", "crop_box": "", "status": "missing_image", "notes": "Render source PDF page before preparing digit crops"})
            continue
        payload: dict[str, Any] = {}
        if raw_path is not None:
            with raw_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        with Image.open(image_path) as image:
            if not payload:
                payload = {"page_width": image.width, "page_height": image.height, "lines": []}
            template_slot = (
                choice_no if str(target.get("form_type", "")).strip() == "5_18" else None
            )
            form_type = str(target.get("form_type", "")).strip()
            if form_type == "5_18_partylist":
                crop_box = partylist_template_vote_cell_crop_box(
                    payload,
                    choice_no=choice_no,
                    image_width=image.width,
                    image_height=image.height,
                )
                if crop_box is None:
                    crop_box = vote_cell_crop_box(
                        payload,
                        choice_no=choice_no,
                        image_width=image.width,
                        image_height=image.height,
                    )
            else:
                crop_box = vote_cell_crop_box(
                    payload,
                    choice_no=choice_no,
                    image_width=image.width,
                    image_height=image.height,
                    template_slot=template_slot,
                    prefer_template=template_slot is not None,
                )
            if crop_box is None and raw_path is None:
                rows.append({**base_record, "crop_variant": "", "crop_path": "", "crop_box": "", "status": "missing_raw_ocr", "notes": "Raw OCR JSON is required to locate this non-template table row anchor"})
                continue
        if crop_box is None:
            rows.append({**base_record, "crop_variant": "", "crop_path": "", "crop_box": "", "status": "missing_or_invalid_row_anchor", "notes": "Could not locate a usable choice row anchor in the table zone"})
            continue

        crop_base = (
            output_dir
            / _safe_part(target.get("form_type", ""))
            / _safe_part(_source_stem(target.get("source_pdf", "")))
            / f"row_{int(target.get('row_index', 0)):06d}_page_{source_page:04d}_choice_{choice_no}"
        )
        for variant, crop_path in _save_crop_variants(image_path, crop_box, crop_base):
            rows.append(
                {
                    **base_record,
                    "crop_variant": variant,
                    "crop_path": str(crop_path),
                    "crop_box": ",".join(str(value) for value in crop_box),
                    "status": "ok",
                    "notes": "",
                }
            )
    return pd.DataFrame(rows, columns=MANIFEST_COLUMNS)


def write_digit_crop_manifest(
    config: ProjectConfig,
    *,
    max_targets: int | None = None,
    row_indexes: set[int] | None = None,
) -> Path:
    config.ensure_output_dirs()
    output_path = (
        config.output("p0_digit_crops_manifest")
        if "p0_digit_crops_manifest" in config.outputs
        else config.path("processed_dir") / "p0_digit_crops_manifest.csv"
    )
    build_digit_crop_manifest(
        config,
        max_targets=max_targets,
        row_indexes=row_indexes,
    ).to_csv(
        output_path,
        index=False,
        encoding="utf-8-sig",
    )
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare digit-only cell crops for P0 missing vote rows.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    parser.add_argument("--max-targets", type=int, default=None)
    parser.add_argument(
        "--row-indexes-csv",
        type=Path,
        default=None,
        help="Optional CSV containing row_index values to crop from the P0 missing-votes queue.",
    )
    args = parser.parse_args()
    print(
        write_digit_crop_manifest(
            load_config(args.config),
            max_targets=args.max_targets,
            row_indexes=_read_row_index_filter(args.row_indexes_csv),
        )
    )


if __name__ == "__main__":
    main()
