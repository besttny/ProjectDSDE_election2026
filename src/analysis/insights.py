from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.pipeline.config import ProjectConfig, load_config
from src.pipeline.schema import RESULT_COLUMNS


def _read_results(config: ProjectConfig) -> pd.DataFrame:
    path = config.output("election_results")
    if not path.exists():
        return pd.DataFrame(columns=RESULT_COLUMNS)
    return pd.read_csv(path)


def _save_top_choices_figure(df: pd.DataFrame, output_dir: Path) -> Path | None:
    if df.empty or "choice_name" not in df or "votes" not in df:
        return None
    top = (
        df.groupby("choice_name", dropna=False)["votes"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    if top.empty:
        return None
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "top_choices.png"
    fig, ax = plt.subplots(figsize=(10, 6))
    top.sort_values().plot(kind="barh", ax=ax, color="#176B87")
    ax.set_title("Top choices by total votes")
    ax.set_xlabel("Votes")
    ax.set_ylabel("")
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path


def build_insights(config: ProjectConfig) -> Path:
    config.ensure_output_dirs()
    df = _read_results(config)
    report_path = config.output("insights_report")
    report_path.parent.mkdir(parents=True, exist_ok=True)

    if df.empty:
        report_path.write_text(
            "# Insights Report\n\n"
            "No parsed election rows are available yet. Add ECT PDFs, run OCR, then rerun the pipeline.\n",
            encoding="utf-8",
        )
        return report_path

    numeric_votes = pd.to_numeric(df["votes"], errors="coerce").fillna(0)
    total_votes = int(numeric_votes.sum())
    stations = int(df["polling_station_no"].dropna().nunique())
    forms = ", ".join(sorted(df["form_type"].dropna().astype(str).unique()))
    needs_review = int((df["validation_status"].astype(str) != "ok").sum())

    top_choices = (
        df.assign(votes=numeric_votes)
        .groupby(["choice_name", "party_name"], dropna=False)["votes"]
        .sum()
        .sort_values(ascending=False)
        .head(5)
        .reset_index()
    )

    station_totals = (
        df.assign(votes=numeric_votes)
        .groupby(["form_type", "polling_station_no"], dropna=False)["votes"]
        .sum()
        .reset_index()
        .sort_values("votes", ascending=False)
        .head(10)
    )

    figure_path = _save_top_choices_figure(df.assign(votes=numeric_votes), config.path("figures_dir"))

    lines = [
        "# Insights Report",
        "",
        "## Coverage",
        f"- Parsed rows: {len(df):,}",
        f"- Parsed polling stations: {stations:,}",
        f"- Forms present: {forms}",
        f"- Total choice votes: {total_votes:,}",
        f"- Rows requiring review: {needs_review:,}",
        "",
        "## Top Choices",
    ]
    for _, row in top_choices.iterrows():
        label = row["choice_name"] or "(missing choice name)"
        party = row["party_name"] or "(missing party)"
        lines.append(f"- {label} / {party}: {int(row['votes']):,} votes")

    lines.extend(["", "## Highest Station/Form Vote Totals"])
    for _, row in station_totals.iterrows():
        lines.append(
            f"- {row['form_type']} station {row['polling_station_no']}: {int(row['votes']):,} votes"
        )

    lines.extend(
        [
            "",
            "## Candidate Insight Angles for Slides",
            "- Compare advance voting forms with election-day forms once all rows are parsed.",
            "- Rank stations by invalid/no-vote rates after summary fields are extracted.",
            "- Use validation failures as data-quality evidence, not hidden cleanup work.",
            "- Add 2023 data to calculate vote swing once external data is loaded.",
        ]
    )
    if figure_path:
        lines.extend(["", f"Generated figure: `{figure_path}`"])
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate insight report and figures.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    args = parser.parse_args()
    print(build_insights(load_config(args.config)))


if __name__ == "__main__":
    main()

