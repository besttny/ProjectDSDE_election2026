from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.config import ProjectConfig
from src.pipeline.manifest import load_manifest
from src.pipeline.schema import NUMERIC_COLUMNS, RESULT_COLUMNS
from src.pipeline.station_inference import build_station_page_map
from src.quality.master_keys import normalize_number_key, normalize_text_key

ELECTION_DAY_CONSTITUENCY_FORM = "5_18"
ELECTION_DAY_PARTYLIST_FORM = "5_18_partylist"


def _source_name(value: object) -> str:
    text = "" if pd.isna(value) else str(value).strip()
    return Path(text).name if text else ""


def _source_name_series(series: pd.Series) -> pd.Series:
    return series.map(_source_name)


def _candidate_master(config: ProjectConfig) -> pd.DataFrame:
    if "master_candidates_file" not in config.paths:
        return pd.DataFrame()
    path = config.path("master_candidates_file")
    if not path.exists():
        return pd.DataFrame()

    frame = pd.read_csv(path).fillna("")
    required = {"province", "constituency_no", "candidate_no", "canonical_name", "party_name"}
    if not required.issubset(frame.columns):
        return pd.DataFrame()

    province = normalize_text_key(config.province)
    constituency_no = normalize_number_key(config.constituency_no)
    working = frame.copy()
    working["_province_key"] = working["province"].map(normalize_text_key)
    working["_constituency_key"] = working["constituency_no"].map(normalize_number_key)
    working["_candidate_key"] = working["candidate_no"].map(normalize_number_key)
    working = working[
        working["_province_key"].eq(province)
        & working["_constituency_key"].eq(constituency_no)
        & working["_candidate_key"].ne("")
    ].copy()
    if working.empty:
        return pd.DataFrame()

    working["_candidate_sort"] = pd.to_numeric(working["_candidate_key"], errors="coerce")
    working = working.sort_values("_candidate_sort", kind="stable")
    return working


def _party_master(config: ProjectConfig) -> pd.DataFrame:
    if "master_parties_file" not in config.paths:
        return pd.DataFrame()
    path = config.path("master_parties_file")
    if not path.exists():
        return pd.DataFrame()

    frame = pd.read_csv(path).fillna("")
    required = {"party_no", "canonical_name"}
    if not required.issubset(frame.columns):
        return pd.DataFrame()

    working = frame.copy()
    working["_party_key"] = working["party_no"].map(normalize_number_key)
    working = working[working["_party_key"].ne("")].copy()
    if working.empty:
        return pd.DataFrame()

    working["_party_sort"] = pd.to_numeric(working["_party_key"], errors="coerce")
    working = working.sort_values("_party_sort", kind="stable")
    return working


def _manifest_source_paths(config: ProjectConfig) -> dict[str, str]:
    return {entry.file_path.name: str(entry.file_path) for entry in load_manifest(config)}


def _station_pages(config: ProjectConfig, *, form_type: str) -> pd.DataFrame:
    page_map = build_station_page_map(config)
    if not page_map:
        return pd.DataFrame()

    source_paths = _manifest_source_paths(config)
    records: list[dict[str, object]] = []
    for (mapped_form_type, source_name, page), info in page_map.items():
        if mapped_form_type != form_type:
            continue
        records.append(
            {
                "form_type": info["form_type"],
                "source_name": source_name,
                "source_pdf": source_paths.get(source_name, source_name),
                "source_page": int(page),
                "polling_station_no": int(info["polling_station_no"]),
                "district": info.get("district", ""),
                "subdistrict": info.get("subdistrict", ""),
            }
        )
    if not records:
        return pd.DataFrame()

    pages = pd.DataFrame(records)
    table_pages = (
        pages.sort_values(["polling_station_no", "source_name", "source_page"], kind="stable")
        .groupby(["polling_station_no", "source_name"], dropna=False, as_index=False)
        .first()
    )
    return table_pages.sort_values(["polling_station_no", "source_page"], kind="stable")


def _best_numeric(series: pd.Series) -> object:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return pd.NA
    return numeric.iloc[0]


def _first_filled(series: pd.Series) -> object:
    for value in series:
        if pd.notna(value) and str(value).strip() != "":
            return value
    return pd.NA


def _valid_observed_rows(
    df: pd.DataFrame,
    *,
    form_type: str,
    choice_keys: set[str],
) -> pd.DataFrame:
    observed = df[df["form_type"].astype(str).eq(form_type)].copy()
    if observed.empty:
        return observed
    observed["_choice_key"] = observed["choice_no"].map(normalize_number_key)
    observed = observed[observed["_choice_key"].isin(choice_keys)].copy()
    if observed.empty:
        return observed

    observed["_has_votes"] = pd.to_numeric(observed["votes"], errors="coerce").notna()
    observed["_is_ok"] = observed["validation_status"].astype(str).eq("ok")
    observed["_confidence_sort"] = pd.to_numeric(observed["ocr_confidence"], errors="coerce").fillna(0)
    observed = observed.sort_values(
        ["polling_station_no", "_choice_key", "_is_ok", "_has_votes", "_confidence_sort"],
        ascending=[True, True, False, False, False],
        kind="stable",
    )
    return observed


def _copy_observed_values(row: dict[str, Any], observed: pd.Series) -> dict[str, Any]:
    for column in [
        "eligible_voters",
        "ballots_cast",
        "valid_votes",
        "invalid_votes",
        "no_vote",
        "votes",
        "ocr_confidence",
    ]:
        value = observed.get(column, "")
        if pd.notna(value) and str(value).strip() != "":
            row[column] = value
    for column in ["source_pdf", "source_page", "district", "subdistrict"]:
        value = observed.get(column, "")
        if pd.notna(value) and str(value).strip() != "":
            row[column] = value
    row["validation_status"] = (
        "ok"
        if pd.notna(pd.to_numeric(pd.Series([row["votes"]]), errors="coerce").iloc[0])
        else "needs_review"
    )
    return row


def _observed_lookup(observed: pd.DataFrame) -> dict[tuple[str, str], pd.Series]:
    lookup: dict[tuple[str, str], pd.Series] = {}
    if observed.empty:
        return lookup
    for _, group in observed.groupby(["polling_station_no", "_choice_key"], dropna=False, sort=False):
        station_key = normalize_number_key(group["polling_station_no"].iloc[0])
        choice_key = str(group["_choice_key"].iloc[0])
        best = group.iloc[0].copy()
        for column in NUMERIC_COLUMNS:
            if column in {"choice_no", "polling_station_no", "constituency_no", "source_page"}:
                continue
            best[column] = _best_numeric(group[column])
        for column in RESULT_COLUMNS:
            if column in NUMERIC_COLUMNS or column not in group.columns:
                continue
            best[column] = _first_filled(group[column])
        lookup[(station_key, choice_key)] = best
    return lookup


def _record_from_expected(
    *,
    config: ProjectConfig,
    station: pd.Series,
    candidate: pd.Series,
    observed: pd.Series | None,
) -> dict[str, Any]:
    candidate_no = normalize_number_key(candidate["candidate_no"])
    row = {column: "" for column in RESULT_COLUMNS}
    row.update(
        {
            "province": config.province,
            "constituency_no": config.constituency_no,
            "form_type": ELECTION_DAY_CONSTITUENCY_FORM,
            "vote_type": "constituency",
            "polling_station_no": station["polling_station_no"],
            "district": station.get("district", ""),
            "subdistrict": station.get("subdistrict", ""),
            "choice_no": int(candidate_no),
            "choice_name": str(candidate["canonical_name"]).strip(),
            "party_name": str(candidate["party_name"]).strip(),
            "source_pdf": station.get("source_pdf", ""),
            "source_page": station.get("source_page", ""),
            "ocr_engine": "master_expected_5_18",
            "ocr_confidence": "",
            "validation_status": "needs_review",
        }
    )
    if observed is None:
        return row

    row["ocr_engine"] = str(observed.get("ocr_engine", "") or "master_expected_5_18")
    return _copy_observed_values(row, observed)


def _party_record_from_expected(
    *,
    config: ProjectConfig,
    station: pd.Series,
    party: pd.Series,
    observed: pd.Series | None,
) -> dict[str, Any]:
    party_no = normalize_number_key(party["party_no"])
    row = {column: "" for column in RESULT_COLUMNS}
    row.update(
        {
            "province": config.province,
            "constituency_no": config.constituency_no,
            "form_type": ELECTION_DAY_PARTYLIST_FORM,
            "vote_type": "party_list",
            "polling_station_no": station["polling_station_no"],
            "district": station.get("district", ""),
            "subdistrict": station.get("subdistrict", ""),
            "choice_no": int(party_no),
            "choice_name": "",
            "party_name": str(party["canonical_name"]).strip(),
            "source_pdf": station.get("source_pdf", ""),
            "source_page": station.get("source_page", ""),
            "ocr_engine": "master_expected_5_18_partylist",
            "ocr_confidence": "",
            "validation_status": "needs_review",
        }
    )
    if observed is None:
        return row

    row["ocr_engine"] = str(observed.get("ocr_engine", "") or "master_expected_5_18_partylist")
    return _copy_observed_values(row, observed)


def _replace_form_rows(
    df: pd.DataFrame,
    *,
    form_type: str,
    stations: pd.DataFrame,
    expected: pd.DataFrame,
) -> pd.DataFrame:
    source_names = _source_name_series(df["source_pdf"])
    station_source_names = set(stations["source_name"].astype(str))
    covered = df["form_type"].astype(str).eq(form_type) & source_names.isin(station_source_names)
    kept = df[~covered].copy()
    return pd.concat([kept, expected], ignore_index=True)[RESULT_COLUMNS]


def _apply_expected_candidate_rows(df: pd.DataFrame, config: ProjectConfig) -> pd.DataFrame:
    candidates = _candidate_master(config)
    stations = _station_pages(config, form_type=ELECTION_DAY_CONSTITUENCY_FORM)
    if candidates.empty or stations.empty:
        return df

    candidate_keys = set(candidates["_candidate_key"].astype(str))
    observed = _valid_observed_rows(
        df,
        form_type=ELECTION_DAY_CONSTITUENCY_FORM,
        choice_keys=candidate_keys,
    )
    lookup = _observed_lookup(observed)

    records: list[dict[str, Any]] = []
    for _, station in stations.iterrows():
        station_key = normalize_number_key(station["polling_station_no"])
        for _, candidate in candidates.iterrows():
            choice_key = str(candidate["_candidate_key"])
            records.append(
                _record_from_expected(
                    config=config,
                    station=station,
                    candidate=candidate,
                    observed=lookup.get((station_key, choice_key)),
                )
            )

    expected = pd.DataFrame(records, columns=RESULT_COLUMNS)
    return _replace_form_rows(
        df,
        form_type=ELECTION_DAY_CONSTITUENCY_FORM,
        stations=stations,
        expected=expected,
    )


def _apply_expected_partylist_rows(df: pd.DataFrame, config: ProjectConfig) -> pd.DataFrame:
    parties = _party_master(config)
    stations = _station_pages(config, form_type=ELECTION_DAY_PARTYLIST_FORM)
    if parties.empty or stations.empty:
        return df

    party_keys = set(parties["_party_key"].astype(str))
    observed = _valid_observed_rows(
        df,
        form_type=ELECTION_DAY_PARTYLIST_FORM,
        choice_keys=party_keys,
    )
    lookup = _observed_lookup(observed)

    records: list[dict[str, Any]] = []
    for _, station in stations.iterrows():
        station_key = normalize_number_key(station["polling_station_no"])
        for _, party in parties.iterrows():
            choice_key = str(party["_party_key"])
            records.append(
                _party_record_from_expected(
                    config=config,
                    station=station,
                    party=party,
                    observed=lookup.get((station_key, choice_key)),
                )
            )

    expected = pd.DataFrame(records, columns=RESULT_COLUMNS)
    return _replace_form_rows(
        df,
        form_type=ELECTION_DAY_PARTYLIST_FORM,
        stations=stations,
        expected=expected,
    )


def apply_expected_5_18_rows(df: pd.DataFrame, config: ProjectConfig) -> pd.DataFrame:
    """Replace noisy 5/18 OCR rows with master-scaffolded rows.

    Election-day constituency rows use the official candidate master scoped by
    province + constituency_no + candidate_no. Party-list rows use the official
    party master scoped by party_no. OCR is still used for numeric values, but
    row identity comes from master data and station/page inference.
    """

    if df.empty:
        return df
    repaired = _apply_expected_candidate_rows(df, config)
    return _apply_expected_partylist_rows(repaired, config)


apply_expected_5_18_candidate_rows = apply_expected_5_18_rows
