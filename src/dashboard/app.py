from __future__ import annotations

import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.pipeline.config import load_config  # noqa: E402


CONFIG_PATH = ROOT / "configs" / "chaiyaphum_2.yaml"


@st.cache_data(show_spinner=False)
def load_dashboard_data() -> pd.DataFrame:
    config = load_config(CONFIG_PATH)
    parquet_path = config.output("dashboard_dataset")
    csv_path = config.output("dashboard_dataset_csv")
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if csv_path.exists():
        return pd.read_csv(csv_path)
    results_path = config.output("election_results")
    if results_path.exists():
        return pd.read_csv(results_path)
    return pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_validation_report() -> pd.DataFrame:
    config = load_config(CONFIG_PATH)
    path = config.output("validation_report")
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_accuracy_report() -> pd.DataFrame:
    config = load_config(CONFIG_PATH)
    path = config.output("accuracy_report")
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_review_queue() -> pd.DataFrame:
    config = load_config(CONFIG_PATH)
    path = config.output("review_queue")
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _format_int(value: float | int) -> str:
    if pd.isna(value):
        return "-"
    return f"{int(value):,}"


def _filter_data(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filters")
    filtered = df.copy()

    for column, label in [
        ("form_type", "Form"),
        ("vote_type", "Vote type"),
        ("validation_status", "Quality status"),
    ]:
        if column in filtered.columns and not filtered.empty:
            values = sorted(filtered[column].dropna().astype(str).unique())
            selected = st.sidebar.multiselect(label, values, default=values)
            if selected:
                filtered = filtered[filtered[column].astype(str).isin(selected)]

    search = st.sidebar.text_input("Search candidate or party")
    if search and not filtered.empty:
        candidate = filtered.get("choice_name", pd.Series(dtype=str)).astype(str)
        party = filtered.get("party_name", pd.Series(dtype=str)).astype(str)
        filtered = filtered[candidate.str.contains(search, case=False, na=False) | party.str.contains(search, case=False, na=False)]

    return filtered


def render_empty_state() -> None:
    st.title("Election 2026 Dashboard")
    st.info(
        "No dashboard dataset is available yet. Place ECT PDFs under `data/raw/pdfs/`, "
        "then run `python -m src.pipeline.run_all --config configs/chaiyaphum_2.yaml`."
    )
    validation = load_validation_report()
    if not validation.empty:
        st.subheader("Latest validation report")
        st.dataframe(validation, use_container_width=True, hide_index=True)


def render_kpis(df: pd.DataFrame) -> None:
    votes = pd.to_numeric(df.get("votes", pd.Series(dtype=float)), errors="coerce").fillna(0)
    stations = df.get("polling_station_no", pd.Series(dtype=float)).dropna().nunique()
    forms = df.get("form_type", pd.Series(dtype=str)).dropna().nunique()
    review_rows = int((df.get("validation_status", pd.Series(dtype=str)).astype(str) != "ok").sum())

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total choice votes", _format_int(votes.sum()))
    col2.metric("Polling stations", _format_int(stations))
    col3.metric("Forms", _format_int(forms))
    col4.metric("Rows to review", _format_int(review_rows))


def render_charts(df: pd.DataFrame) -> None:
    if df.empty:
        return
    working = df.copy()
    working["votes"] = pd.to_numeric(working["votes"], errors="coerce").fillna(0)

    st.subheader("Top candidates / parties")
    top = (
        working.groupby(["choice_name", "party_name"], dropna=False)["votes"]
        .sum()
        .reset_index()
        .sort_values("votes", ascending=False)
        .head(15)
    )
    top["label"] = top["choice_name"].fillna("").astype(str)
    top.loc[top["label"].str.len() == 0, "label"] = top["party_name"].fillna("(missing)")
    chart = (
        alt.Chart(top)
        .mark_bar(color="#176B87")
        .encode(
            x=alt.X("votes:Q", title="Votes"),
            y=alt.Y("label:N", sort="-x", title=""),
            tooltip=["choice_name", "party_name", "votes"],
        )
        .properties(height=420)
    )
    st.altair_chart(chart, use_container_width=True)

    st.subheader("Station/form vote totals")
    station = (
        working.groupby(["form_type", "polling_station_no"], dropna=False)["votes"]
        .sum()
        .reset_index()
        .sort_values("votes", ascending=False)
        .head(30)
    )
    station["station_label"] = (
        station["form_type"].astype(str) + " / station " + station["polling_station_no"].astype(str)
    )
    station_chart = (
        alt.Chart(station)
        .mark_bar(color="#D95F59")
        .encode(
            x=alt.X("votes:Q", title="Votes"),
            y=alt.Y("station_label:N", sort="-x", title=""),
            tooltip=["form_type", "polling_station_no", "votes"],
        )
        .properties(height=520)
    )
    st.altair_chart(station_chart, use_container_width=True)


def render_quality() -> None:
    validation = load_validation_report()
    accuracy = load_accuracy_report()
    review_queue = load_review_queue()
    if validation.empty and accuracy.empty and review_queue.empty:
        return
    st.subheader("Data quality checks")
    if not validation.empty:
        st.caption("Validation report")
        st.dataframe(validation, use_container_width=True, hide_index=True)
    if not accuracy.empty:
        st.caption("Accuracy report")
        st.dataframe(accuracy, use_container_width=True, hide_index=True)
    if not review_queue.empty:
        st.caption("Manual review queue")
        st.dataframe(review_queue.head(50), use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Election 2026 Dashboard", layout="wide")
    st.markdown(
        """
        <style>
        .block-container { padding-top: 2rem; }
        [data-testid="stMetric"] { border: 1px solid #e5e7eb; padding: 14px 16px; border-radius: 8px; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    df = load_dashboard_data()
    if df.empty:
        render_empty_state()
        return

    st.title("Election 2026: Chaiyaphum Constituency 2")
    st.caption("Structured OCR results, validation status, and analysis-ready election metrics.")
    filtered = _filter_data(df)
    render_kpis(filtered)
    render_quality()
    render_charts(filtered)

    st.subheader("Result rows")
    st.dataframe(filtered, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
