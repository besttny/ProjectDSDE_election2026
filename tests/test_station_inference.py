import json
from pathlib import Path

import pandas as pd

from src.pipeline.config import ProjectConfig
from src.pipeline.manifest import ManifestEntry
import src.pipeline.station_inference as station_inference
from src.pipeline.station_inference import _auto_page_assignments


def _write_page(raw_dir: Path, page: int, text: str) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    payload = {"lines": [{"text": text}]}
    (raw_dir / f"sample_page_{page:04d}.json").write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )


def test_auto_page_assignments_reassigns_party_block_before_next_constituency_start(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    constituency_start = "ส.ส. 5/18 รายงานผลการนับคะแนนสมาชิกสภาผู้แทนราษฎรแบบแบ่งเขตเลือกตั้ง จำนวนบัตร"
    constituency_sign = "ส.ส. 5/18 ประกาศ ณ วันที่ ประธานกรรมการประจำหน่วยเลือกตั้ง"
    party_start = "ส.ส. 5/18 (บช) รายงานผลการนับคะแนนสมาชิกสภาผู้แทนราษฎรแบบบัญชีรายชื่อ ตามที่ได้มีพระราชกฤษฎีกา"
    party_table = "ส.ส. 5/18 (บช) หมายเลขของบัญชีรายชื่อ พรรคการเมือง ได้คะแนน"
    party_sign = "ส.ส. 5/18 (บช) ประกาศ ณ วันที่ ประธานกรรมการประจำหน่วยเลือกตั้ง"

    for page, text in {
        1: constituency_start,
        2: constituency_sign,
        3: party_start,
        4: party_table,
        5: party_table,
        6: party_sign,
        7: constituency_start,
        8: constituency_sign,
        9: party_start,
        10: party_table,
        11: party_table,
        12: party_sign,
        13: party_start,
        14: party_table,
        15: party_table,
        16: party_sign,
        17: constituency_start,
        18: constituency_sign,
    }.items():
        _write_page(raw_dir, page, text)

    assignments = _auto_page_assignments(raw_dir)

    assert assignments[9]["form_type"] == "5_18_partylist"
    assert assignments[9]["station_group_local"] == 2
    assert assignments[13]["form_type"] == "5_18_partylist"
    assert assignments[13]["station_group_local"] == 3
    assert assignments[17]["form_type"] == "5_18"
    assert assignments[17]["station_group_local"] == 3


def test_auto_page_assignments_cascades_when_party_block_was_preassigned(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    constituency_start = "ส.ส. 5/18 รายงานผลการนับคะแนนสมาชิกสภาผู้แทนราษฎรแบบแบ่งเขตเลือกตั้ง จำนวนบัตร"
    constituency_sign = "ส.ส. 5/18 ประกาศ ณ วันที่ ประธานกรรมการประจำหน่วยเลือกตั้ง"
    party_start = "ส.ส. 5/18 (บช) รายงานผลการนับคะแนนสมาชิกสภาผู้แทนราษฎรแบบบัญชีรายชื่อ ตามที่ได้มีพระราชกฤษฎีกา"
    party_table = "ส.ส. 5/18 (บช) หมายเลขของบัญชีรายชื่อ พรรคการเมือง ได้คะแนน"
    party_sign = "ส.ส. 5/18 (บช) ประกาศ ณ วันที่ ประธานกรรมการประจำหน่วยเลือกตั้ง"

    for page, text in {
        1: constituency_start,
        2: constituency_sign,
        3: party_start,
        4: party_table,
        5: party_table,
        6: party_sign,
        7: party_start,
        8: party_table,
        9: party_table,
        10: party_sign,
        11: constituency_start,
        12: constituency_sign,
        13: party_start,
        14: party_table,
        15: party_table,
        16: party_sign,
        17: constituency_start,
        18: constituency_sign,
    }.items():
        _write_page(raw_dir, page, text)

    assignments = _auto_page_assignments(raw_dir)

    assert assignments[7]["form_type"] == "5_18_partylist"
    assert assignments[7]["station_group_local"] == 2
    assert assignments[11]["form_type"] == "5_18"
    assert assignments[11]["station_group_local"] == 2
    assert assignments[13]["form_type"] == "5_18_partylist"
    assert assignments[13]["station_group_local"] == 3
    assert assignments[17]["form_type"] == "5_18"
    assert assignments[17]["station_group_local"] == 3


def test_apply_station_inference_preserves_manual_review_station_ids(
    tmp_path: Path,
    monkeypatch,
):
    raw_dir = tmp_path / "data/raw/ocr/5_18_auto/sample"
    _write_page(
        raw_dir,
        1,
        "ส.ส. 5/18 รายงานผลการนับคะแนนสมาชิกสภาผู้แทนราษฎรแบบแบ่งเขตเลือกตั้ง จำนวนบัตร",
    )

    config = ProjectConfig(
        root=tmp_path,
        data={
            "paths": {
                "raw_ocr_dir": "data/raw/ocr",
            }
        },
    )
    entry = ManifestEntry(
        form_type="5_18_auto",
        vote_type="mixed",
        required=True,
        expected_polling_stations=1,
        file_path=Path("sample.pdf"),
        source_url="",
        notes="",
    )
    monkeypatch.setattr(station_inference, "load_manifest", lambda _config: [entry])

    rows = pd.DataFrame(
        [
            {
                "form_type": "5_18",
                "vote_type": "constituency",
                "polling_station_no": 46,
                "source_pdf": "sample.pdf",
                "source_page": 1,
                "ocr_engine": "manual_review",
            },
            {
                "form_type": "5_18",
                "vote_type": "constituency",
                "polling_station_no": 99,
                "source_pdf": "sample.pdf",
                "source_page": 1,
                "ocr_engine": "paddleocr+zones",
            },
        ]
    )

    output = station_inference.apply_station_inference(rows, config)

    assert output.loc[0, "polling_station_no"] == 46
    assert output.loc[1, "polling_station_no"] == 1
