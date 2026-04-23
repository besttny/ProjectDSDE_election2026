import json
from pathlib import Path

import pandas as pd

from src.pipeline.config import ProjectConfig
from src.pipeline.digit_vote_fallback import apply_raw_digit_vote_fallback
from src.pipeline.schema import RESULT_COLUMNS


def _config(root: Path) -> ProjectConfig:
    return ProjectConfig(
        root=root,
        data={
            "project": {"province": "ชัยภูมิ", "constituency_no": 2},
            "paths": {
                "raw_ocr_dir": "data/raw/ocr",
                "master_candidates_file": "data/external/master_candidates.csv",
                "master_parties_file": "data/external/master_parties.csv",
            },
            "quality": {"auto_digit_fallback_min_confidence": 0.75},
        },
    )


def _write_master_files(root: Path) -> None:
    external = root / "data/external"
    external.mkdir(parents=True)
    (external / "master_candidates.csv").write_text(
        "province,constituency_no,candidate_no,canonical_name,party_name\n"
        "ชัยภูมิ,2,1,นาย ก,พรรค ก\n",
        encoding="utf-8",
    )
    (external / "master_parties.csv").write_text(
        "party_no,canonical_name\n35,รักชาติ\n",
        encoding="utf-8",
    )


def _result_row(**updates: object) -> dict[str, object]:
    row = {column: "" for column in RESULT_COLUMNS}
    row.update(
        {
            "province": "ชัยภูมิ",
            "constituency_no": 2,
            "form_type": "5_18",
            "polling_station_no": 1,
            "choice_no": 1,
            "source_pdf": "/content/data/raw/pdfs/sample.pdf",
            "source_page": 1,
            "validation_status": "needs_review",
        }
    )
    row.update(updates)
    return row


def _write_raw_ocr(root: Path, *, text: str, confidence: float, y: int = 2180) -> None:
    raw_dir = root / "data/raw/ocr/5_18/sample"
    raw_dir.mkdir(parents=True)
    (raw_dir / "sample_page_0001.json").write_text(
        json.dumps(
            {
                "page_width": 2480,
                "page_height": 3509,
                "lines": [
                    {
                        "text": text,
                        "confidence": confidence,
                        "bbox": [[1500, y], [1580, y], [1580, y + 50], [1500, y + 50]],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_apply_raw_digit_vote_fallback_applies_unique_high_confidence_value(tmp_path: Path):
    _write_master_files(tmp_path)
    _write_raw_ocr(tmp_path, text="42", confidence=0.91)
    df = pd.DataFrame([_result_row()], columns=RESULT_COLUMNS)

    filled, audit = apply_raw_digit_vote_fallback(df, _config(tmp_path))

    assert int(filled.loc[0, "votes"]) == 42
    assert filled.loc[0, "ocr_engine"] == "raw_ocr_digit_fallback"
    assert filled.loc[0, "validation_status"] == "ok"
    assert audit.loc[0, "status"] == "applied"


def test_apply_raw_digit_vote_fallback_skips_low_confidence_value(tmp_path: Path):
    _write_master_files(tmp_path)
    _write_raw_ocr(tmp_path, text="42", confidence=0.5)
    df = pd.DataFrame([_result_row()], columns=RESULT_COLUMNS)

    filled, audit = apply_raw_digit_vote_fallback(df, _config(tmp_path))

    assert pd.isna(pd.to_numeric(pd.Series([filled.loc[0, "votes"]]), errors="coerce")).iloc[0]
    assert audit.loc[0, "status"] == "low_confidence"


def test_apply_raw_digit_vote_fallback_applies_high_confidence_zero_marker(tmp_path: Path):
    _write_master_files(tmp_path)
    _write_raw_ocr(tmp_path, text="..", confidence=0.91)
    df = pd.DataFrame([_result_row()], columns=RESULT_COLUMNS)

    filled, audit = apply_raw_digit_vote_fallback(df, _config(tmp_path))

    assert int(filled.loc[0, "votes"]) == 0
    assert filled.loc[0, "validation_status"] == "ok"
    assert audit.loc[0, "status"] == "applied_zero_marker"


def test_apply_raw_digit_vote_fallback_does_not_turn_low_confidence_digits_into_zero(tmp_path: Path):
    _write_master_files(tmp_path)
    _write_raw_ocr(tmp_path, text="2", confidence=0.5)
    df = pd.DataFrame([_result_row()], columns=RESULT_COLUMNS)

    filled, audit = apply_raw_digit_vote_fallback(df, _config(tmp_path))

    assert pd.isna(pd.to_numeric(pd.Series([filled.loc[0, "votes"]]), errors="coerce")).iloc[0]
    assert audit.loc[0, "status"] == "low_confidence"


def test_apply_raw_digit_vote_fallback_uses_partylist_template(tmp_path: Path):
    _write_master_files(tmp_path)
    raw_dir = tmp_path / "data/raw/ocr/5_18_partylist/sample"
    raw_dir.mkdir(parents=True)
    (raw_dir / "sample_page_0001.json").write_text(
        json.dumps(
            {
                "page_width": 2480,
                "page_height": 3509,
                "lines": [
                    {
                        "text": "7",
                        "confidence": 0.93,
                        "bbox": [[1340, 1000], [1400, 1000], [1400, 1060], [1340, 1060]],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    df = pd.DataFrame(
        [
            _result_row(
                form_type="5_18_partylist",
                choice_no=35,
                source_pdf="/content/data/raw/pdfs/sample.pdf",
                source_page=1,
            )
        ],
        columns=RESULT_COLUMNS,
    )

    filled, audit = apply_raw_digit_vote_fallback(df, _config(tmp_path))

    assert int(filled.loc[0, "votes"]) == 7
    assert audit.loc[0, "status"] == "applied"


def test_apply_raw_digit_vote_fallback_rejects_out_of_range_value(tmp_path: Path):
    _write_master_files(tmp_path)
    _write_raw_ocr(tmp_path, text="2623", confidence=0.99)
    df = pd.DataFrame([_result_row()], columns=RESULT_COLUMNS)

    filled, audit = apply_raw_digit_vote_fallback(df, _config(tmp_path))

    assert pd.isna(pd.to_numeric(pd.Series([filled.loc[0, "votes"]]), errors="coerce")).iloc[0]
    assert audit.loc[0, "status"] == "out_of_range"
