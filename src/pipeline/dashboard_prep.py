from __future__ import annotations

import argparse

import pandas as pd

from src.pipeline.clean import clean_results
from src.pipeline.config import ProjectConfig, load_config
from src.pipeline.schema import RESULT_COLUMNS


def build_dashboard_dataset(config: ProjectConfig) -> tuple[str, str]:
    config.ensure_output_dirs()
    results_path = config.output("election_results")
    if not results_path.exists():
        clean_results(config)

    if results_path.exists():
        df = pd.read_csv(results_path)
    else:
        df = pd.DataFrame(columns=RESULT_COLUMNS)

    if not df.empty:
        df["turnout_rate"] = pd.to_numeric(df["ballots_cast"], errors="coerce") / pd.to_numeric(
            df["eligible_voters"], errors="coerce"
        )
        df["invalid_rate"] = pd.to_numeric(df["invalid_votes"], errors="coerce") / pd.to_numeric(
            df["ballots_cast"], errors="coerce"
        )
        df["no_vote_rate"] = pd.to_numeric(df["no_vote"], errors="coerce") / pd.to_numeric(
            df["ballots_cast"], errors="coerce"
        )
    else:
        df["turnout_rate"] = pd.Series(dtype=float)
        df["invalid_rate"] = pd.Series(dtype=float)
        df["no_vote_rate"] = pd.Series(dtype=float)

    parquet_path = config.output("dashboard_dataset")
    csv_path = config.output("dashboard_dataset_csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception:
        parquet_path = csv_path
    return str(parquet_path), str(csv_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Streamlit dashboard dataset.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    args = parser.parse_args()
    print("\n".join(build_dashboard_dataset(load_config(args.config))))


if __name__ == "__main__":
    main()

