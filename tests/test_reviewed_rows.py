from pathlib import Path

import pandas as pd

from src.pipeline.reviewed_rows import apply_reviewed_rows


def test_apply_reviewed_rows_replaces_same_source_page_and_form(tmp_path: Path):
    parsed = pd.DataFrame(
        [
            {
                "province": "ชัยภูมิ",
                "form_type": "5_18",
                "source_pdf": "/repo/data/raw/pdfs/sample.pdf",
                "source_page": 1,
                "choice_no": 1,
                "votes": 3,
            },
            {
                "province": "ชัยภูมิ",
                "form_type": "5_18_partylist",
                "source_pdf": "/repo/data/raw/pdfs/sample.pdf",
                "source_page": 1,
                "choice_no": 1,
                "votes": 9,
            },
        ]
    )
    reviewed_path = tmp_path / "reviewed_rows.csv"
    reviewed_path.write_text(
        "province,form_type,source_pdf,source_page,choice_no,votes,validation_status\n"
        "ชัยภูมิ,5_18,data/raw/pdfs/sample.pdf,1,1,13,ok\n",
        encoding="utf-8",
    )

    output = apply_reviewed_rows(parsed, reviewed_path)

    assert len(output) == 2
    reviewed = output[output["form_type"] == "5_18"].iloc[0]
    partylist = output[output["form_type"] == "5_18_partylist"].iloc[0]
    assert reviewed["votes"] == 13
    assert partylist["votes"] == 9
