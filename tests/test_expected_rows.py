import json
from pathlib import Path

import pandas as pd

from src.pipeline.config import ProjectConfig
from src.pipeline.expected_rows import apply_expected_5_18_rows
from src.pipeline.schema import RESULT_COLUMNS


def _config(root: Path, expected_polling_stations: int = 0) -> ProjectConfig:
    return ProjectConfig(
        root=root,
        data={
            "project": {
                "province": "ชัยภูมิ",
                "constituency_no": 2,
                "expected_polling_stations": expected_polling_stations,
            },
            "paths": {
                "manifest": "configs/manifest.csv",
                "raw_ocr_dir": "data/raw/ocr",
                "master_candidates_file": "data/external/master_candidates.csv",
                "master_parties_file": "data/external/master_parties.csv",
            },
        },
    )


def _write_fixed_5_18_inputs(root: Path) -> None:
    (root / "configs").mkdir(parents=True)
    (root / "configs/manifest.csv").write_text(
        "form_type,vote_type,required,expected_polling_stations,file_path,source_url,notes\n"
        "5_18,constituency,true,1,data/raw/pdfs/sample.pdf,,\n",
        encoding="utf-8",
    )
    (root / "data/raw/pdfs").mkdir(parents=True)
    (root / "data/raw/pdfs/sample.pdf").write_bytes(b"%PDF-1.4\n")
    raw_dir = root / "data/raw/ocr/5_18/sample"
    raw_dir.mkdir(parents=True)
    payload = {"lines": [], "source_pdf": "data/raw/pdfs/sample.pdf"}
    (raw_dir / "sample_page_0001.json").write_text(json.dumps(payload), encoding="utf-8")
    (raw_dir / "sample_page_0002.json").write_text(json.dumps(payload), encoding="utf-8")

    external = root / "data/external"
    external.mkdir(parents=True)
    (external / "master_candidates.csv").write_text(
        "province,constituency_no,form_type,candidate_no,canonical_name,party_name,aliases\n"
        "ชัยภูมิ,2,518,1,นาย ก,พรรค ก,\n"
        "ชัยภูมิ,2,518,2,นาง ข,พรรค ข,\n",
        encoding="utf-8",
    )


def _write_missing_station_inputs(root: Path) -> None:
    (root / "configs").mkdir(parents=True)
    (root / "configs/manifest.csv").write_text(
        "form_type,vote_type,required,expected_polling_stations,file_path,source_url,notes\n"
        "5_18,constituency,true,2,data/raw/pdfs/sample.pdf,,\n",
        encoding="utf-8",
    )
    (root / "data/raw/pdfs").mkdir(parents=True)
    (root / "data/raw/pdfs/sample.pdf").write_bytes(b"%PDF-1.4\n")

    external = root / "data/external"
    external.mkdir(parents=True)
    (external / "master_candidates.csv").write_text(
        "province,constituency_no,form_type,candidate_no,canonical_name,party_name,aliases\n"
        "ชัยภูมิ,2,518,1,นาย ก,พรรค ก,\n",
        encoding="utf-8",
    )


def test_apply_expected_5_18_candidate_rows_replaces_invalid_ocr_choice(tmp_path: Path):
    _write_fixed_5_18_inputs(tmp_path)
    rows = []
    for row in [
        {
            "province": "ชัยภูมิ",
            "constituency_no": 2,
            "form_type": "5_18",
            "vote_type": "constituency",
            "polling_station_no": 1,
            "choice_no": 1,
            "choice_name": "นาย OCR",
            "party_name": "พรรค OCR",
            "votes": 42,
            "valid_votes": 50,
            "source_pdf": str(tmp_path / "data/raw/pdfs/sample.pdf"),
            "source_page": 1,
            "ocr_engine": "paddleocr",
            "ocr_confidence": 0.9,
            "validation_status": "ok",
        },
        {
            "province": "ชัยภูมิ",
            "constituency_no": 2,
            "form_type": "5_18",
            "vote_type": "constituency",
            "polling_station_no": 1,
            "choice_no": 9,
            "choice_name": "ABC",
            "party_name": "XYZ",
            "votes": "",
            "source_pdf": str(tmp_path / "data/raw/pdfs/sample.pdf"),
            "source_page": 1,
            "ocr_engine": "paddleocr",
            "ocr_confidence": 0.9,
            "validation_status": "needs_review",
        },
    ]:
        full = {column: "" for column in RESULT_COLUMNS}
        full.update(row)
        rows.append(full)
    df = pd.DataFrame(rows, columns=RESULT_COLUMNS)

    repaired = apply_expected_5_18_rows(df, _config(tmp_path))

    assert repaired["choice_no"].tolist() == [1, 2]
    assert repaired.loc[0, "choice_name"] == "นาย ก"
    assert repaired.loc[0, "party_name"] == "พรรค ก"
    assert int(repaired.loc[0, "votes"]) == 42
    assert repaired.loc[0, "validation_status"] == "ok"
    assert repaired.loc[1, "choice_name"] == "นาง ข"
    assert repaired.loc[1, "validation_status"] == "needs_review"


def test_apply_expected_5_18_candidate_rows_scaffolds_missing_expected_stations(tmp_path: Path):
    _write_missing_station_inputs(tmp_path)
    unrelated = {column: "" for column in RESULT_COLUMNS}
    unrelated.update(
        {
            "province": "ชัยภูมิ",
            "constituency_no": 2,
            "form_type": "5_16",
            "vote_type": "constituency",
            "choice_no": 1,
            "choice_name": "นาย ก",
            "party_name": "พรรค ก",
            "votes": 10,
            "validation_status": "ok",
        }
    )

    repaired = apply_expected_5_18_rows(
        pd.DataFrame([unrelated], columns=RESULT_COLUMNS),
        _config(tmp_path, expected_polling_stations=2),
    )
    expected_rows = repaired[repaired["form_type"].astype(str).eq("5_18")]

    assert expected_rows["polling_station_no"].tolist() == [1, 2]
    assert expected_rows["choice_no"].tolist() == [1, 1]
    assert expected_rows["source_pdf"].fillna("").tolist() == ["", ""]
    assert expected_rows["source_page"].fillna("").tolist() == ["", ""]
    assert expected_rows["validation_status"].tolist() == ["needs_review", "needs_review"]


def test_apply_expected_5_18_partylist_rows_use_party_master(tmp_path: Path):
    (tmp_path / "configs").mkdir(parents=True)
    (tmp_path / "configs/manifest.csv").write_text(
        "form_type,vote_type,required,expected_polling_stations,file_path,source_url,notes\n"
        "5_18_partylist,party_list,true,1,data/raw/pdfs/party.pdf,,\n",
        encoding="utf-8",
    )
    (tmp_path / "data/raw/pdfs").mkdir(parents=True)
    (tmp_path / "data/raw/pdfs/party.pdf").write_bytes(b"%PDF-1.4\n")
    raw_dir = tmp_path / "data/raw/ocr/5_18_partylist/party"
    raw_dir.mkdir(parents=True)
    payload = {"lines": [], "source_pdf": "data/raw/pdfs/party.pdf"}
    for page in range(1, 5):
        (raw_dir / f"party_page_{page:04d}.json").write_text(
            json.dumps(payload),
            encoding="utf-8",
        )

    external = tmp_path / "data/external"
    external.mkdir(parents=True)
    (external / "master_parties.csv").write_text(
        "party_no,canonical_name\n"
        "1,พรรคหนึ่ง\n"
        "2,พรรคสอง\n",
        encoding="utf-8",
    )

    rows = []
    for row in [
        {
            "province": "ชัยภูมิ",
            "constituency_no": 2,
            "form_type": "5_18_partylist",
            "vote_type": "party_list",
            "polling_station_no": 1,
            "choice_no": 1,
            "party_name": "พรรค OCR",
            "votes": 12,
            "source_pdf": str(tmp_path / "data/raw/pdfs/party.pdf"),
            "source_page": 1,
            "ocr_engine": "paddleocr",
            "ocr_confidence": 0.9,
            "validation_status": "ok",
        },
        {
            "province": "ชัยภูมิ",
            "constituency_no": 2,
            "form_type": "5_18_partylist",
            "vote_type": "party_list",
            "polling_station_no": 1,
            "choice_no": 99,
            "party_name": "Noise",
            "votes": "",
            "source_pdf": str(tmp_path / "data/raw/pdfs/party.pdf"),
            "source_page": 1,
            "ocr_engine": "paddleocr",
            "ocr_confidence": 0.9,
            "validation_status": "needs_review",
        },
    ]:
        full = {column: "" for column in RESULT_COLUMNS}
        full.update(row)
        rows.append(full)
    df = pd.DataFrame(rows, columns=RESULT_COLUMNS)

    repaired = apply_expected_5_18_rows(df, _config(tmp_path))

    assert repaired["choice_no"].tolist() == [1, 2]
    assert repaired.loc[0, "party_name"] == "พรรคหนึ่ง"
    assert int(repaired.loc[0, "votes"]) == 12
    assert repaired.loc[1, "party_name"] == "พรรคสอง"
    assert repaired.loc[1, "validation_status"] == "needs_review"
