from pathlib import Path

import pandas as pd

from src.pipeline.clean import apply_manual_corrections, apply_master_names
from src.pipeline.config import ProjectConfig


def test_apply_manual_corrections_keeps_row_count(tmp_path: Path):
    df = pd.DataFrame(
        [
            {"choice_no": 1, "choice_name": "นาย ก", "party_name": "พรคข้อมูล"},
            {"choice_no": 2, "choice_name": "นาง ข", "party_name": "พรรคตัวอย่าง"},
        ]
    )
    corrections = tmp_path / "manual_corrections.csv"
    corrections.write_text(
        "match_column,match_value,target_column,new_value,reason\n"
        "party_name,พรคข้อมูล,party_name,พรรคข้อมูล,OCR missing character\n",
        encoding="utf-8",
    )

    corrected = apply_manual_corrections(df, corrections)

    assert len(corrected) == len(df)
    assert corrected.loc[0, "party_name"] == "พรรคข้อมูล"
    assert corrected.loc[1, "party_name"] == "พรรคตัวอย่าง"


def test_apply_master_names_scopes_constituency_candidates_by_province_and_zone(tmp_path: Path):
    external = tmp_path / "data/external"
    external.mkdir(parents=True)
    (external / "master_candidates.csv").write_text(
        "province,constituency_no,form_type,candidate_no,canonical_name,party_name,aliases\n"
        "ชัยภูมิ,2,518,1,นาย ก เขตสอง,พรรค ก,\n"
        "ชัยภูมิ,3,518,1,นาย ข เขตสาม,พรรค ข,\n",
        encoding="utf-8",
    )
    (external / "master_parties.csv").write_text(
        "party_no,canonical_name,aliases\n"
        "1,พรรคบัญชีรายชื่อหนึ่ง,\n",
        encoding="utf-8",
    )
    config = ProjectConfig(
        root=tmp_path,
        data={
            "project": {"province": "ชัยภูมิ", "constituency_no": 2},
            "paths": {
                "master_candidates_file": "data/external/master_candidates.csv",
                "master_parties_file": "data/external/master_parties.csv",
            },
        },
    )
    df = pd.DataFrame(
        [
            {
                "province": "ชัยภูมิ",
                "constituency_no": 2,
                "form_type": "5_18",
                "choice_no": 1,
                "choice_name": "",
                "party_name": "",
            },
            {
                "province": "ชัยภูมิ",
                "constituency_no": 3,
                "form_type": "5_18",
                "choice_no": 1,
                "choice_name": "",
                "party_name": "",
            },
            {
                "province": "ชัยภูมิ",
                "constituency_no": 2,
                "form_type": "5_18_partylist",
                "choice_no": 1,
                "choice_name": "OCR noise",
                "party_name": "",
            },
        ]
    )

    filled = apply_master_names(df, config)

    assert filled.loc[0, "choice_name"] == "นาย ก เขตสอง"
    assert filled.loc[0, "party_name"] == "พรรค ก"
    assert filled.loc[1, "choice_name"] == "นาย ข เขตสาม"
    assert filled.loc[1, "party_name"] == "พรรค ข"
    assert filled.loc[2, "choice_name"] == ""
    assert filled.loc[2, "party_name"] == "พรรคบัญชีรายชื่อหนึ่ง"
