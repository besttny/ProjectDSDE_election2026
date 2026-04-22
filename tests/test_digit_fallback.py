from pathlib import Path

import pandas as pd

from src.ocr import digit_fallback
from src.pipeline.config import ProjectConfig


def _config(root: Path) -> ProjectConfig:
    return ProjectConfig(
        root=root,
        data={
            "project": {"province": "ชัยภูมิ", "constituency_no": 2},
            "paths": {
                "processed_dir": "data/processed",
                "raw_image_dir": "data/raw/images",
                "raw_ocr_dir": "data/raw/ocr",
                "parsed_dir": "data/processed/parsed",
                "figures_dir": "outputs/figures",
                "reports_dir": "outputs/reports",
                "master_candidates_file": "data/external/master_candidates.csv",
                "master_parties_file": "data/external/master_parties.csv",
            },
            "outputs": {
                "p0_digit_crops_manifest": "data/processed/p0_digit_crops_manifest.csv",
                "digit_crop_ocr_suggestions": "data/processed/digit_crop_ocr_suggestions.csv",
            },
        },
    )


def _write_master_files(root: Path) -> None:
    external = root / "data/external"
    external.mkdir(parents=True)
    (external / "master_candidates.csv").write_text(
        "province,constituency_no,candidate_no,canonical_name,party_name\n"
        "ชัยภูมิ,2,2,นาย ก,พรรค ก\n",
        encoding="utf-8",
    )
    (external / "master_parties.csv").write_text(
        "party_no,canonical_name\n1,พรรคหนึ่ง\n",
        encoding="utf-8",
    )


def test_digit_crop_suggestions_skip_invalid_choice_no(tmp_path: Path):
    _write_master_files(tmp_path)
    processed = tmp_path / "data/processed"
    processed.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "row_index": 7,
                "form_type": "5_18",
                "source_pdf": "sample.pdf",
                "source_page": 1,
                "polling_station_no": 1,
                "choice_no": 7,
                "choice_key_status": "invalid",
                "crop_variant": "",
                "crop_path": "",
                "status": "invalid_choice_no",
            }
        ]
    ).to_csv(processed / "p0_digit_crops_manifest.csv", index=False, encoding="utf-8-sig")

    suggestions = digit_fallback.build_digit_crop_suggestions(_config(tmp_path), tesseract_bin=None)

    assert suggestions.loc[0, "status"] == "skipped_invalid_choice_no"
    assert suggestions.loc[0, "selected_votes"] == ""


def test_digit_crop_suggestions_record_consensus_candidate(monkeypatch, tmp_path: Path):
    _write_master_files(tmp_path)
    processed = tmp_path / "data/processed"
    crop_dir = tmp_path / "data/raw/crops"
    processed.mkdir(parents=True)
    crop_dir.mkdir(parents=True)
    crop_path = crop_dir / "cell.png"
    crop_path.write_bytes(b"not a real image because OCR is mocked")
    pd.DataFrame(
        [
            {
                "row_index": 8,
                "form_type": "5_18",
                "source_pdf": "sample.pdf",
                "source_page": 1,
                "polling_station_no": 1,
                "choice_no": 2,
                "choice_key_status": "valid",
                "crop_variant": "threshold3x",
                "crop_path": str(crop_path),
                "status": "ok",
            }
        ]
    ).to_csv(processed / "p0_digit_crops_manifest.csv", index=False, encoding="utf-8-sig")

    monkeypatch.setattr(digit_fallback, "_run_tesseract_digits", lambda *args, **kwargs: "42")

    suggestions = digit_fallback.build_digit_crop_suggestions(
        _config(tmp_path),
        tesseract_bin="/bin/tesseract",
        psms=(7,),
    )

    assert suggestions.loc[0, "status"] == "candidate_suggestion"
    assert int(suggestions.loc[0, "selected_votes"]) == 42
