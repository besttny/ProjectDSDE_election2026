import json
from pathlib import Path

import pandas as pd
from PIL import Image

from src.ocr.digit_crops import build_digit_crop_manifest, vote_cell_crop_box
from src.pipeline.config import ProjectConfig


def _config(root: Path) -> ProjectConfig:
    return ProjectConfig(
        root=root,
        data={
            "paths": {
                "raw_image_dir": "data/raw/images",
                "raw_ocr_dir": "data/raw/ocr",
                "processed_dir": "data/processed",
            },
            "outputs": {
                "review_queue": "data/processed/review_queue.csv",
                "p0_digit_crops_manifest": "data/processed/p0_digit_crops_manifest.csv",
            },
        },
    )


def _payload() -> dict:
    return {
        "page_width": 1000,
        "page_height": 1400,
        "zones": [{"name": "table", "crop_box": [80, 700, 940, 1320]}],
        "lines": [
            {
                "text": "2",
                "zone": "table",
                "bbox": [[120, 860], [140, 860], [140, 890], [120, 890]],
            }
        ],
    }


def test_vote_cell_crop_box_uses_choice_anchor_and_right_vote_column():
    box = vote_cell_crop_box(_payload(), choice_no=2, image_width=1000, image_height=1400)

    assert box is not None
    assert box[0] > 550
    assert box[2] <= 940
    assert box[1] < 875 < box[3]


def test_vote_cell_crop_box_scales_payload_coordinates_to_image_size():
    box = vote_cell_crop_box(_payload(), choice_no=2, image_width=500, image_height=700)

    assert box is not None
    assert 275 < box[0] < 340
    assert box[2] <= 470
    assert box[1] < 438 < box[3]


def test_vote_cell_crop_box_rejects_tiny_edge_crop():
    payload = {
        "page_width": 1000,
        "page_height": 1400,
        "zones": [{"name": "table", "crop_box": [990, 700, 1000, 1320]}],
        "lines": [
            {
                "text": "2",
                "zone": "table",
                "bbox": [[991, 860], [993, 860], [993, 890], [991, 890]],
            }
        ],
    }

    box = vote_cell_crop_box(payload, choice_no=2, image_width=1000, image_height=1400)

    assert box is None


def test_build_digit_crop_manifest_creates_three_preprocessed_variants(tmp_path: Path):
    config = _config(tmp_path)
    image_dir = tmp_path / "data/raw/images/5_18/sample"
    raw_dir = tmp_path / "data/raw/ocr/5_18/sample"
    processed_dir = tmp_path / "data/processed"
    image_dir.mkdir(parents=True)
    raw_dir.mkdir(parents=True)
    processed_dir.mkdir(parents=True)

    Image.new("RGB", (1000, 1400), "white").save(image_dir / "sample_page_0001.png")
    (raw_dir / "sample_page_0001.json").write_text(
        json.dumps(_payload(), ensure_ascii=False),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "priority": "P0",
                "reason": "missing_votes",
                "row_index": 7,
                "source_pdf": "/content/data/raw/pdfs/sample.pdf",
                "source_page": 1,
                "form_type": "5_18",
                "polling_station_no": 1,
                "choice_no": 2,
            }
        ]
    ).to_csv(processed_dir / "review_queue.csv", index=False, encoding="utf-8-sig")

    manifest = build_digit_crop_manifest(config)

    assert manifest["status"].tolist() == ["ok", "ok", "ok"]
    assert set(manifest["crop_variant"]) == {"raw", "gray2x", "threshold3x"}
    for path in manifest["crop_path"]:
        assert Path(path).exists()
