from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
EXTERNAL = ROOT / "data" / "external"
SOURCE_DIR = EXTERNAL / "Election66"
OUTPUT_DIR = SOURCE_DIR / "processed"

PROVINCE_NAME = "ชัยภูมิ"
PROVINCE_ID = 36
CONSTITUENCY_NO = 2

SPECIAL_REGISTRARS = {
    "ล่วงหน้าในเขตเลือกตั้ง": "advance_in_constituency",
    "ล่วงหน้านอกเขตเลือกตั้ง": "advance_out_constituency",
}

SUMMARY_NAMES = {
    "ผู้มีสิทธิ์",
    "ผู้มาใช้สิทธิ์",
    "บัตรเสีย",
    "ไม่เลือกผู้ใด",
}


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8-sig")


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def build_reference_area(stations: pd.DataFrame) -> tuple[set[tuple[str, str]], dict[str, str]]:
    area_pairs = set(zip(stations["district"].astype(str), stations["subdistrict"].astype(str)))

    subdistrict_lookup: dict[str, str] = {}
    for subdistrict, group in stations.groupby("subdistrict"):
        districts = sorted(group["district"].dropna().astype(str).unique())
        if len(districts) == 1:
            subdistrict_lookup[str(subdistrict)] = districts[0]

    return area_pairs, subdistrict_lookup


def add_area_columns(
    scores: pd.DataFrame,
    area_pairs: set[tuple[str, str]],
) -> pd.DataFrame:
    df = scores.copy()
    df["district"] = df["district"].where(df["district"].notna(), "")
    df["subdistrict"] = df["subdistrict"].where(df["subdistrict"].notna(), "")
    df["district"] = df["district"].astype(str)
    df["subdistrict"] = df["subdistrict"].astype(str)
    df["area_key"] = df["district"] + "||" + df["subdistrict"]
    df["in_project_area"] = [
        (district, subdistrict) in area_pairs
        for district, subdistrict in zip(df["district"], df["subdistrict"])
    ]

    def classify(row: pd.Series) -> str:
        if row["in_project_area"]:
            return "project_area"
        registrar = str(row.get("registrar", ""))
        return SPECIAL_REGISTRARS.get(registrar, "outside_reference")

    df["area_scope"] = df.apply(classify, axis=1)
    return df


def normalize_locations(
    locations: pd.DataFrame,
    area_pairs: set[tuple[str, str]],
    subdistrict_lookup: dict[str, str],
) -> pd.DataFrame:
    df = locations.copy()
    df = df.rename(
        columns={
            "districtname": "raw_district",
            "subdistrictname": "raw_subdistrict",
        }
    )
    df["raw_district"] = df["raw_district"].astype(str)
    df["raw_subdistrict"] = df["raw_subdistrict"].astype(str)

    project_district = []
    project_subdistrict = []
    in_project_area = []

    for raw_district, raw_subdistrict in zip(df["raw_district"], df["raw_subdistrict"]):
        raw_pair = (raw_district, raw_subdistrict)
        if raw_pair in area_pairs:
            district = raw_district
            subdistrict = raw_subdistrict
            is_project = True
        elif raw_subdistrict in subdistrict_lookup:
            district = subdistrict_lookup[raw_subdistrict]
            subdistrict = raw_subdistrict
            is_project = (district, subdistrict) in area_pairs
        else:
            district = raw_district
            subdistrict = raw_subdistrict
            is_project = False

        project_district.append(district)
        project_subdistrict.append(subdistrict)
        in_project_area.append(is_project)

    df["district"] = project_district
    df["subdistrict"] = project_subdistrict
    df["area_key"] = df["district"] + "||" + df["subdistrict"]
    df["in_project_area"] = in_project_area
    return df


def station_summary(scores: pd.DataFrame) -> pd.DataFrame:
    summary = scores[
        [
            "id",
            "index_no",
            "document",
            "province",
            "province_number",
            "district",
            "subdistrict",
            "registrar",
            "station_number",
            "area_key",
            "area_scope",
            "in_project_area",
            "เขต_ผู้มีสิทธิ์",
            "เขต_ผู้มาใช้สิทธิ์",
            "เขต_บัตรเสีย",
            "เขต_ไม่เลือกผู้ใด",
            "บช_ผู้มีสิทธิ์",
            "บช_ผู้มาใช้สิทธิ์",
            "บช_บัตรเสีย",
            "บช_ไม่เลือกผู้ใด",
        ]
    ].copy()
    summary = summary.rename(
        columns={
            "province_number": "constituency_no",
            "เขต_ผู้มีสิทธิ์": "constituency_eligible_voters",
            "เขต_ผู้มาใช้สิทธิ์": "constituency_voters_present",
            "เขต_บัตรเสีย": "constituency_spoiled_ballots",
            "เขต_ไม่เลือกผู้ใด": "constituency_no_vote",
            "บช_ผู้มีสิทธิ์": "party_list_eligible_voters",
            "บช_ผู้มาใช้สิทธิ์": "party_list_voters_present",
            "บช_บัตรเสีย": "party_list_spoiled_ballots",
            "บช_ไม่เลือกผู้ใด": "party_list_no_vote",
        }
    )
    return summary


def candidate_reference(candidates: pd.DataFrame) -> pd.DataFrame:
    df = candidates.copy()
    df.insert(0, "province", PROVINCE_NAME)
    df.insert(1, "constituency_no", CONSTITUENCY_NO)
    return df.rename(columns={"vote_count": "official_candidate_total"})


def candidate_votes_long(scores: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
    id_columns = [
        "id",
        "index_no",
        "document",
        "province",
        "province_number",
        "district",
        "subdistrict",
        "registrar",
        "station_number",
        "area_key",
        "area_scope",
        "in_project_area",
    ]

    rows = []
    for candidate in candidates.itertuples(index=False):
        vote_column = f"เขต_{candidate.party_name}"
        if vote_column not in scores.columns:
            continue
        subset = scores[id_columns].copy()
        subset["candidate_no"] = candidate.candidate_no
        subset["candidate_name"] = candidate.candidate_name
        subset["party_name"] = candidate.party_name
        subset["votes"] = scores[vote_column].fillna(0).astype(int)
        rows.append(subset)

    if not rows:
        return pd.DataFrame(columns=id_columns + ["candidate_no", "candidate_name", "party_name", "votes"])

    result = pd.concat(rows, ignore_index=True)
    return result.rename(columns={"province_number": "constituency_no"})


def party_votes_long(scores: pd.DataFrame) -> pd.DataFrame:
    id_columns = [
        "id",
        "index_no",
        "document",
        "province",
        "province_number",
        "district",
        "subdistrict",
        "registrar",
        "station_number",
        "area_key",
        "area_scope",
        "in_project_area",
    ]
    party_columns = [
        col
        for col in scores.columns
        if col.startswith("บช_") and col.removeprefix("บช_") not in SUMMARY_NAMES
    ]
    long_df = scores.melt(
        id_vars=id_columns,
        value_vars=party_columns,
        var_name="party_name",
        value_name="votes",
    )
    long_df["party_name"] = long_df["party_name"].str.removeprefix("บช_")
    long_df["votes"] = long_df["votes"].fillna(0).astype(int)
    return long_df.rename(columns={"province_number": "constituency_no"})


def totals_from_long(
    long_df: pd.DataFrame,
    group_columns: list[str],
) -> pd.DataFrame:
    return (
        long_df.groupby(group_columns, as_index=False)["votes"]
        .sum()
        .sort_values("votes", ascending=False, ignore_index=True)
    )


def area_summary(
    scores_area: pd.DataFrame,
    candidate_long_area: pd.DataFrame,
    party_long_area: pd.DataFrame,
) -> pd.DataFrame:
    base = (
        station_summary(scores_area)
        .groupby(["district", "subdistrict", "area_key"], as_index=False)
        .agg(
            station_count=("id", "count"),
            eligible_voters=("constituency_eligible_voters", "sum"),
            voters_present=("constituency_voters_present", "sum"),
            spoiled_ballots=("constituency_spoiled_ballots", "sum"),
            no_vote=("constituency_no_vote", "sum"),
        )
    )
    base["turnout_pct"] = (base["voters_present"] / base["eligible_voters"] * 100).round(2)

    candidate_winners = (
        candidate_long_area.groupby(["district", "subdistrict", "candidate_name", "party_name"], as_index=False)[
            "votes"
        ]
        .sum()
        .sort_values(["district", "subdistrict", "votes"], ascending=[True, True, False])
        .drop_duplicates(["district", "subdistrict"])
        .rename(
            columns={
                "candidate_name": "constituency_winner",
                "party_name": "constituency_winner_party",
                "votes": "constituency_winner_votes",
            }
        )
    )
    party_winners = (
        party_long_area.groupby(["district", "subdistrict", "party_name"], as_index=False)["votes"]
        .sum()
        .sort_values(["district", "subdistrict", "votes"], ascending=[True, True, False])
        .drop_duplicates(["district", "subdistrict"])
        .rename(columns={"party_name": "party_list_winner", "votes": "party_list_winner_votes"})
    )

    result = base.merge(candidate_winners, on=["district", "subdistrict"], how="left")
    result = result.merge(party_winners, on=["district", "subdistrict"], how="left")
    return result.sort_values(["district", "subdistrict"], ignore_index=True)


def main() -> None:
    stations = read_csv(EXTERNAL / "stations.csv")
    scores = read_csv(SOURCE_DIR / "election_scores_2566.csv")
    locations = read_csv(SOURCE_DIR / "election_locations_66.csv")
    candidates = read_csv(SOURCE_DIR / "candidate66.csv")
    candidate_ref = candidate_reference(candidates)

    area_pairs, subdistrict_lookup = build_reference_area(stations)

    scores_filtered = scores[
        scores["province"].eq(PROVINCE_NAME) & scores["province_number"].eq(CONSTITUENCY_NO)
    ].copy()
    scores_filtered = add_area_columns(scores_filtered, area_pairs)
    scores_area = scores_filtered[scores_filtered["in_project_area"]].copy()

    locations_filtered = locations[
        locations["provinceid"].eq(PROVINCE_ID) & locations["divisionnumber"].eq(CONSTITUENCY_NO)
    ].copy()
    locations_filtered = normalize_locations(locations_filtered, area_pairs, subdistrict_lookup)

    station_df = station_summary(scores_filtered)
    candidate_long = candidate_votes_long(scores_filtered, candidates)
    party_long = party_votes_long(scores_filtered)
    candidate_long_area = candidate_long[candidate_long["in_project_area"]].copy()
    party_long_area = party_long[party_long["in_project_area"]].copy()

    candidate_totals = totals_from_long(
        candidate_long,
        ["candidate_no", "candidate_name", "party_name"],
    )
    candidate_totals = candidate_totals.merge(
        candidate_ref[
            [
                "candidate_no",
                "candidate_name",
                "party_name",
                "official_candidate_total",
            ]
        ],
        on=["candidate_no", "candidate_name", "party_name"],
        how="left",
    )
    candidate_totals["official_difference"] = (
        candidate_totals["votes"] - candidate_totals["official_candidate_total"]
    )

    party_totals = totals_from_long(party_long, ["party_name"])
    area_df = area_summary(scores_area, candidate_long_area, party_long_area)

    write_csv(scores_filtered, OUTPUT_DIR / "chaiyaphum_2_scores_2566.csv")
    write_csv(scores_area, OUTPUT_DIR / "chaiyaphum_2_scores_area_2566.csv")
    write_csv(locations_filtered, OUTPUT_DIR / "chaiyaphum_2_locations_66.csv")
    write_csv(candidate_ref, OUTPUT_DIR / "chaiyaphum_2_candidates_2566.csv")
    write_csv(station_df, OUTPUT_DIR / "chaiyaphum_2_station_summary_2566.csv")
    write_csv(candidate_long, OUTPUT_DIR / "chaiyaphum_2_candidate_votes_long_2566.csv")
    write_csv(candidate_long_area, OUTPUT_DIR / "chaiyaphum_2_candidate_votes_area_long_2566.csv")
    write_csv(party_long, OUTPUT_DIR / "chaiyaphum_2_party_votes_long_2566.csv")
    write_csv(party_long_area, OUTPUT_DIR / "chaiyaphum_2_party_votes_area_long_2566.csv")
    write_csv(candidate_totals, OUTPUT_DIR / "chaiyaphum_2_candidate_totals_2566.csv")
    write_csv(party_totals, OUTPUT_DIR / "chaiyaphum_2_party_totals_2566.csv")
    write_csv(area_df, OUTPUT_DIR / "chaiyaphum_2_area_summary_2566.csv")

    print(f"Input scores: {len(scores):,} rows")
    print(f"Chaiyaphum constituency 2 scores: {len(scores_filtered):,} rows")
    print(f"Candidate reference: {len(candidate_ref):,} rows")
    print(f"Project-area station rows: {len(scores_area):,} rows")
    print(f"Location rows: {len(locations_filtered):,} rows")
    print(f"Area summary: {len(area_df):,} subdistricts")
    print(f"Output directory: {OUTPUT_DIR.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
