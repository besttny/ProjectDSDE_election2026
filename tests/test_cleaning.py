from pathlib import Path

import pandas as pd

from src.pipeline.clean import apply_manual_corrections


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

