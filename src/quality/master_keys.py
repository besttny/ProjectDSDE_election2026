from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd

from src.pipeline.config import ProjectConfig

CONSTITUENCY_FORMS = {"5_16", "5_17", "5_18"}
PARTYLIST_FORMS = {"5_16_partylist", "5_17_partylist", "5_18_partylist"}

ChoiceValidation = Literal["valid", "invalid", "unknown"]


def normalize_number_key(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.notna(numeric) and float(numeric).is_integer():
        return str(int(numeric))
    return "" if pd.isna(value) else str(value).strip()


def normalize_text_key(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).replace("\ufeff", "").strip()


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path).fillna("") if path.exists() else pd.DataFrame()


def _optional_config_path(config: ProjectConfig, key: str) -> Path | None:
    if key not in config.paths:
        return None
    return config.path(key)


def candidate_master_keys(config: ProjectConfig) -> set[tuple[str, str, str]]:
    path = _optional_config_path(config, "master_candidates_file")
    if path is None:
        return set()
    frame = _read_csv(path)
    required = {"province", "constituency_no", "candidate_no"}
    if frame.empty or not required.issubset(frame.columns):
        return set()

    keys: set[tuple[str, str, str]] = set()
    for _, row in frame.iterrows():
        province = normalize_text_key(row["province"])
        constituency_no = normalize_number_key(row["constituency_no"])
        candidate_no = normalize_number_key(row["candidate_no"])
        if province and constituency_no and candidate_no:
            keys.add((province, constituency_no, candidate_no))
    return keys


def party_master_keys(config: ProjectConfig) -> set[str]:
    path = _optional_config_path(config, "master_parties_file")
    if path is None:
        return set()
    frame = _read_csv(path)
    if frame.empty or "party_no" not in frame.columns:
        return set()
    return {
        party_no
        for party_no in (normalize_number_key(value) for value in frame["party_no"])
        if party_no
    }


def candidate_key_for_values(
    config: ProjectConfig,
    *,
    province: object = "",
    constituency_no: object = "",
    choice_no: object = "",
) -> tuple[str, str, str]:
    return (
        normalize_text_key(province) or normalize_text_key(config.province),
        normalize_number_key(constituency_no) or normalize_number_key(config.constituency_no),
        normalize_number_key(choice_no),
    )


def validate_choice_key(
    config: ProjectConfig,
    *,
    form_type: object,
    choice_no: object,
    province: object = "",
    constituency_no: object = "",
) -> ChoiceValidation:
    """Validate a parsed choice number against the official master data.

    ``unknown`` means the relevant master file is unavailable or incomplete, so
    callers should avoid blocking work only because local setup is partial.
    """

    form = str(form_type).strip()
    choice_key = normalize_number_key(choice_no)
    if not choice_key:
        return "invalid"

    if form in CONSTITUENCY_FORMS:
        keys = candidate_master_keys(config)
        if not keys:
            return "unknown"
        key = candidate_key_for_values(
            config,
            province=province,
            constituency_no=constituency_no,
            choice_no=choice_key,
        )
        return "valid" if key in keys else "invalid"

    if form in PARTYLIST_FORMS:
        keys = party_master_keys(config)
        if not keys:
            return "unknown"
        return "valid" if choice_key in keys else "invalid"

    return "unknown"


def choice_key_is_usable(
    config: ProjectConfig,
    *,
    form_type: object,
    choice_no: object,
    province: object = "",
    constituency_no: object = "",
) -> bool:
    return validate_choice_key(
        config,
        form_type=form_type,
        choice_no=choice_no,
        province=province,
        constituency_no=constituency_no,
    ) in {"valid", "unknown"}
