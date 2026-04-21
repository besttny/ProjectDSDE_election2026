from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.pipeline.clean import clean_results
from src.pipeline.config import ProjectConfig, load_config
from src.pipeline.schema import RESULT_COLUMNS

CONSTITUENCY_COLUMNS = [
    "province",
    "zone",
    "form_type",
    "voting_type",
    "station_id",
    "station_name",
    "candidate_no",
    "candidate_name",
    "party",
    "votes",
    "total_ballots",
    "invalid_ballots",
    "no_vote_ballots",
]

PARTYLIST_COLUMNS = [
    "province",
    "zone",
    "form_type",
    "voting_type",
    "station_id",
    "party_no",
    "party",
    "votes",
    "total_ballots",
    "invalid_ballots",
    "no_vote_ballots",
]

FORM_MAP = {
    "5_16": ("516", "in_district"),
    "5_17": ("517", "out_district"),
    "5_18": ("518", "election_day"),
    "5_16_partylist": ("516_bch", "in_district"),
    "5_17_partylist": ("517_bch", "out_district"),
    "5_18_partylist": ("518_bch", "election_day"),
}


def _read_results(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=RESULT_COLUMNS)
    return pd.read_csv(path)


def _series(df: pd.DataFrame, column: str, default: object = "") -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([default] * len(df), index=df.index)


def _normalize_station_id(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    return numeric.astype("Int64").astype("string").replace("<NA>", "")


def _normalize_number(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    return numeric.astype("Int64").astype("string").replace("<NA>", "")


def _final_form_type(form_type: str) -> str:
    return FORM_MAP.get(str(form_type), (str(form_type), ""))[0]


def _voting_type(form_type: str) -> str:
    return FORM_MAP.get(str(form_type), ("", ""))[1]


def _station_name(df: pd.DataFrame) -> pd.Series:
    district = _series(df, "district").fillna("").astype(str).str.strip()
    subdistrict = _series(df, "subdistrict").fillna("").astype(str).str.strip()
    combined = (subdistrict + " " + district).str.strip()
    return combined


def build_constituency_votes(df: pd.DataFrame) -> pd.DataFrame:
    rows = df[df["form_type"].isin(["5_16", "5_17", "5_18"])].copy()
    output = pd.DataFrame(index=rows.index)
    output["province"] = _series(rows, "province")
    output["zone"] = _normalize_number(_series(rows, "constituency_no"))
    output["form_type"] = rows["form_type"].map(_final_form_type)
    output["voting_type"] = rows["form_type"].map(_voting_type)
    output["station_id"] = _normalize_station_id(_series(rows, "polling_station_no"))
    output["station_name"] = _station_name(rows)
    output["candidate_no"] = _normalize_number(_series(rows, "choice_no"))
    output["candidate_name"] = _series(rows, "choice_name").fillna("")
    output["party"] = _series(rows, "party_name").fillna("")
    output["votes"] = _normalize_number(_series(rows, "votes"))
    output["total_ballots"] = _normalize_number(_series(rows, "ballots_cast"))
    output["invalid_ballots"] = _normalize_number(_series(rows, "invalid_votes"))
    output["no_vote_ballots"] = _normalize_number(_series(rows, "no_vote"))
    return output[CONSTITUENCY_COLUMNS].reset_index(drop=True)


def build_partylist_votes(df: pd.DataFrame) -> pd.DataFrame:
    rows = df[df["form_type"].isin(["5_16_partylist", "5_17_partylist", "5_18_partylist"])].copy()
    output = pd.DataFrame(index=rows.index)
    output["province"] = _series(rows, "province")
    output["zone"] = _normalize_number(_series(rows, "constituency_no"))
    output["form_type"] = rows["form_type"].map(_final_form_type)
    output["voting_type"] = rows["form_type"].map(_voting_type)
    output["station_id"] = _normalize_station_id(_series(rows, "polling_station_no"))
    output["party_no"] = _normalize_number(_series(rows, "choice_no"))
    party_name = _series(rows, "party_name").fillna("").astype(str)
    choice_name = _series(rows, "choice_name").fillna("").astype(str)
    output["party"] = party_name.where(party_name.str.len() > 0, choice_name)
    output["votes"] = _normalize_number(_series(rows, "votes"))
    output["total_ballots"] = _normalize_number(_series(rows, "ballots_cast"))
    output["invalid_ballots"] = _normalize_number(_series(rows, "invalid_votes"))
    output["no_vote_ballots"] = _normalize_number(_series(rows, "no_vote"))
    return output[PARTYLIST_COLUMNS].reset_index(drop=True)


def export_final_schema(config: ProjectConfig) -> tuple[Path, Path]:
    config.ensure_output_dirs()
    results_path = config.output("election_results")
    if not results_path.exists():
        clean_results(config)
    df = _read_results(results_path)

    constituency = build_constituency_votes(df)
    partylist = build_partylist_votes(df)

    constituency_path = config.output("constituency_votes")
    partylist_path = config.output("partylist_votes")
    constituency_path.parent.mkdir(parents=True, exist_ok=True)
    constituency.to_csv(constituency_path, index=False, encoding="utf-8-sig")
    partylist.to_csv(partylist_path, index=False, encoding="utf-8-sig")
    return constituency_path, partylist_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Export final CSV schema files.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    args = parser.parse_args()
    for path in export_final_schema(load_config(args.config)):
        print(path)


if __name__ == "__main__":
    main()
