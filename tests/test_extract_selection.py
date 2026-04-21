from pathlib import Path

import pytest

from src.ocr.extract import select_manifest_entries
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
