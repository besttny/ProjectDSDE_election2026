from pathlib import Path

import pytest

from src.ocr.extract import (
    _ocr_mode_signature,
    _profile_form_type_for_page,
    _raw_json_supports_current_mode,
    _scope_ocr_profile,
    select_manifest_entries,
)
from src.ocr.form_types import OFFICIAL_OCR_FORM_TYPES
from src.pipeline.config import ProjectConfig, _simple_yaml_load, load_config
from src.pipeline.manifest import ManifestEntry


def _entry(form_type: str, path: str) -> ManifestEntry:
    return ManifestEntry(form_type, "constituency", True, None, Path(path), "", "")


def test_select_manifest_entries_uses_1_based_inclusive_indexes():
    entries = [
        _entry("5_16", "one.pdf"),
        _entry("5_18", "two.pdf"),
        _entry("5_18_partylist", "three.pdf"),
    ]

    selected = select_manifest_entries(entries, start_index=2, end_index=3)

    assert [entry.file_path.name for entry in selected] == ["two.pdf", "three.pdf"]


def test_select_manifest_entries_filters_form_type_and_filename():
    entries = [
        _entry("5_18", "district/a.pdf"),
        _entry("5_18_partylist", "district/a_party.pdf"),
        _entry("5_18", "other/b.pdf"),
    ]

    selected = select_manifest_entries(
        entries,
        form_types=["5_18,5_17"],
        file_contains="district",
    )

    assert [entry.file_path.name for entry in selected] == ["a.pdf"]


def test_select_manifest_entries_rejects_invalid_ranges():
    with pytest.raises(ValueError):
        select_manifest_entries([_entry("5_18", "one.pdf")], start_index=3, end_index=2)


def test_select_manifest_entries_returns_empty_for_unmatched_filter():
    selected = select_manifest_entries(
        [_entry("5_18", "one.pdf")],
        form_types=["5_17"],
    )

    assert selected == []


def test_scope_ocr_profile_applies_zone_and_form_overrides(tmp_path: Path):
    config = ProjectConfig(
        root=tmp_path,
        data={
            "ocr": {
                "primary_engine": "paddleocr",
                "fallback_engine": "",
                "languages": ["th"],
                "profiles": {
                    "default": {"preprocess": "raw", "line_filter": "off"},
                    "zones": {
                        "table": {
                            "preprocess": "table_text",
                            "line_filter": "thai_numeric",
                            "confidence_threshold": 0.45,
                        }
                    },
                    "forms": {
                        "5_18_partylist": {
                            "zones": {"table": {"confidence_threshold": 0.40}}
                        }
                    },
                },
            }
        },
    )

    profile = _scope_ocr_profile(config, form_type="5_18_partylist", zone_name="table")

    assert profile["preprocess"] == "table_text"
    assert profile["line_filter"] == "thai_numeric"
    assert profile["confidence_threshold"] == 0.40
    assert profile["languages"] == ["th"]


def test_profile_form_type_for_mixed_5_18_partylist_page():
    lines = [
        {"text": "แบบรายงานผลการนับคะแนนแบบบัญชีรายชื่อ ส.ส. ๕/๑๘ (บช)"},
        {"text": "จำนวนคะแนนที่แต่ละพรรคการเมืองได้รับ"},
    ]

    assert _profile_form_type_for_page("5_18_auto", lines) == "5_18_partylist"


def test_profile_form_type_for_mixed_5_18_constituency_page():
    lines = [
        {"text": "รายงานผลการนับคะแนนสมาชิกสภาผู้แทนราษฎรแบบแบ่งเขตเลือกตั้ง"},
        {"text": "จำนวนผู้มีสิทธิเลือกตั้งตามบัญชีรายชื่อผู้มีสิทธิเลือกตั้ง"},
    ]

    assert _profile_form_type_for_page("5_18_auto", lines) == "5_18"


def test_profile_form_type_leaves_non_mixed_forms_unchanged():
    assert (
        _profile_form_type_for_page("5_17_partylist", [{"text": "แบบบัญชีรายชื่อ"}])
        == "5_17_partylist"
    )


def test_config_defines_ocr_profiles_for_all_official_form_types():
    config = load_config(Path(__file__).parents[1] / "configs/chaiyaphum_2.yaml")
    forms = config.ocr["profiles"]["forms"]

    for form_type in OFFICIAL_OCR_FORM_TYPES:
        assert form_type in forms
        assert "table" in forms[form_type]["zones"]
        profile = _scope_ocr_profile(config, form_type=form_type, zone_name="table")
        assert profile["languages"] == ["th"]
        assert profile["fallback_engine"] == ""
        assert profile["line_filter"] == "thai_numeric"

    assert (
        _scope_ocr_profile(config, form_type="5_18", zone_name="table")["preprocess"]
        == "election_day_constituency_table_text"
    )
    assert (
        _scope_ocr_profile(config, form_type="5_18_partylist", zone_name="table")[
            "preprocess"
        ]
        == "election_day_partylist_table_text"
    )


def test_fallback_yaml_loader_unquotes_form_profile_keys():
    data = _simple_yaml_load(
        """
ocr:
  profiles:
    forms:
      "5_16":
        zones:
          table:
            preprocess: advance_constituency_table_text
"""
    )

    assert "5_16" in data["ocr"]["profiles"]["forms"]


def test_raw_json_support_check_requires_current_ocr_signature(tmp_path: Path):
    config = ProjectConfig(
        root=tmp_path,
        data={"ocr": {"dpi": 350, "languages": ["th"], "zone_ocr": {"enabled": True}}},
    )
    signature = _ocr_mode_signature(config)
    raw_path = tmp_path / "page.json"
    raw_path.write_text(
        '{"ocr_mode_signature": "' + signature + '", "zones": [{"name": "table"}]}',
        encoding="utf-8",
    )

    assert _raw_json_supports_current_mode(
        raw_path,
        zone_ocr_enabled=True,
        ocr_mode_signature=signature,
    )
    assert not _raw_json_supports_current_mode(
        raw_path,
        zone_ocr_enabled=True,
        ocr_mode_signature="old",
    )
