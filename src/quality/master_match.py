from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.pipeline.clean import clean_results
from src.pipeline.config import ProjectConfig, load_config
from src.pipeline.schema import RESULT_COLUMNS
from src.quality.fuzzy_match import load_master_terms, suggest_value


MASTER_MATCH_COLUMNS = [
    "field",
    "observed_value",
    "suggested_value",
    "score",
    "status",
    "form_type",
    "polling_station_no",
    "choice_no",
    "source_pdf",
    "source_page",
]


def _read_results(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=RESULT_COLUMNS)
    return pd.read_csv(path)


def _candidate_terms(config: ProjectConfig) -> list[str]:
    return load_master_terms(config.path("master_candidates_file"), "canonical_name")


def _party_terms(config: ProjectConfig) -> list[str]:
    return load_master_terms(config.path("master_parties_file"), "canonical_name")


def _station_terms(config: ProjectConfig) -> list[str]:
    path = config.path("master_stations_file")
    terms = load_master_terms(path, "station_name")
    if not path.exists():
        return terms
    frame = pd.read_csv(path).fillna("")
    for column in ["district", "subdistrict"]:
        if column in frame.columns:
            terms.extend(str(value).strip() for value in frame[column] if str(value).strip())
    return sorted(set(terms))


def _add_suggestion(
    rows: list[dict[str, object]],
    *,
    record: pd.Series,
    field: str,
    observed_value: object,
    candidates: list[str],
    threshold: float,
) -> None:
    if not candidates or pd.isna(observed_value) or not str(observed_value).strip():
        return
    suggested_value, score, status = suggest_value(observed_value, candidates, threshold)
    if status == "exact":
        return
    rows.append(
        {
            "field": field,
            "observed_value": observed_value,
            "suggested_value": suggested_value,
            "score": score,
            "status": status,
            "form_type": record.get("form_type", ""),
            "polling_station_no": record.get("polling_station_no", ""),
            "choice_no": record.get("choice_no", ""),
            "source_pdf": record.get("source_pdf", ""),
            "source_page": record.get("source_page", ""),
        }
    )


def build_master_match_report(config: ProjectConfig) -> pd.DataFrame:
    results_path = config.output("election_results")
    if not results_path.exists():
        clean_results(config)
    df = _read_results(results_path)
    threshold = float(config.quality.get("fuzzy_match_threshold", 0.86))
    candidate_terms = _candidate_terms(config)
    party_terms = _party_terms(config)
    station_terms = _station_terms(config)

    rows: list[dict[str, object]] = []
    for _, record in df.iterrows():
        _add_suggestion(
            rows,
            record=record,
            field="choice_name",
            observed_value=record.get("choice_name", ""),
            candidates=candidate_terms,
            threshold=threshold,
        )
        _add_suggestion(
            rows,
            record=record,
            field="party_name",
            observed_value=record.get("party_name", ""),
            candidates=party_terms,
            threshold=threshold,
        )
        station_observed = " ".join(
            part
            for part in [str(record.get("subdistrict", "")).strip(), str(record.get("district", "")).strip()]
            if part
        )
        _add_suggestion(
            rows,
            record=record,
            field="station_name",
            observed_value=station_observed,
            candidates=station_terms,
            threshold=threshold,
        )
    return pd.DataFrame(rows, columns=MASTER_MATCH_COLUMNS)


def write_master_match_report(config: ProjectConfig) -> Path:
    config.ensure_output_dirs()
    output_path = config.output("master_match_report")
    build_master_match_report(config).to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build fuzzy master-data match suggestions.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    args = parser.parse_args()
    print(write_master_match_report(load_config(args.config)))


if __name__ == "__main__":
    main()
