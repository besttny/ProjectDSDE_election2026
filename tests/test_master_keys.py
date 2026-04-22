from pathlib import Path

from src.pipeline.config import ProjectConfig
from src.quality.master_keys import validate_choice_key


def _config(root: Path) -> ProjectConfig:
    return ProjectConfig(
        root=root,
        data={
            "project": {"province": "ชัยภูมิ", "constituency_no": 2},
            "paths": {
                "master_candidates_file": "data/external/master_candidates.csv",
                "master_parties_file": "data/external/master_parties.csv",
            },
        },
    )


def test_validate_choice_key_scopes_constituency_candidates_by_province_and_zone(tmp_path: Path):
    external = tmp_path / "data/external"
    external.mkdir(parents=True)
    (external / "master_candidates.csv").write_text(
        "province,constituency_no,candidate_no,canonical_name,party_name\n"
        "ชัยภูมิ,2,3,นาย ก,เพื่อไทย\n"
        "ชัยภูมิ,3,3,นาย ข,เพื่อไทย\n",
        encoding="utf-8",
    )
    (external / "master_parties.csv").write_text("party_no,canonical_name\n1,พรรคหนึ่ง\n", encoding="utf-8")

    config = _config(tmp_path)

    assert validate_choice_key(config, form_type="5_18", choice_no=3) == "valid"
    assert (
        validate_choice_key(
            config,
            form_type="5_18",
            choice_no=3,
            province="ชัยภูมิ",
            constituency_no=3,
        )
        == "valid"
    )
    assert (
        validate_choice_key(
            config,
            form_type="5_18",
            choice_no=4,
            province="ชัยภูมิ",
            constituency_no=2,
        )
        == "invalid"
    )


def test_validate_choice_key_uses_party_number_for_party_list(tmp_path: Path):
    external = tmp_path / "data/external"
    external.mkdir(parents=True)
    (external / "master_candidates.csv").write_text(
        "province,constituency_no,candidate_no,canonical_name,party_name\n"
        "ชัยภูมิ,2,3,นาย ก,เพื่อไทย\n",
        encoding="utf-8",
    )
    (external / "master_parties.csv").write_text("party_no,canonical_name\n9,เพื่อไทย\n", encoding="utf-8")

    config = _config(tmp_path)

    assert validate_choice_key(config, form_type="5_18_partylist", choice_no=9) == "valid"
    assert validate_choice_key(config, form_type="5_18_partylist", choice_no=3) == "invalid"
