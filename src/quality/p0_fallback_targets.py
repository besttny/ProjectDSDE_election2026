from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.pipeline.config import ProjectConfig, load_config
from src.quality.review_queue import write_review_queue

TARGET_COLUMNS = [
    "priority",
    "source_pdf",
    "source_page",
    "form_type",
    "polling_station_no",
    "row_count",
    "reasons",
    "suggested_zones",
    "suggested_fallback",
]

SUMMARY_REASONS = {"ballot_accounting_mismatch", "choice_votes_exceed_valid_votes"}
METADATA_REASONS = {"missing_station_id"}
TABLE_REASONS = {"missing_votes", "missing_choice_no", "duplicate_choice_row"}


def _read_review_queue(config: ProjectConfig) -> pd.DataFrame:
    path = config.output("review_queue")
    if not path.exists():
        write_review_queue(config)
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _suggested_zones(reasons: set[str]) -> str:
    zones: list[str] = []
    if reasons & METADATA_REASONS:
        zones.append("metadata")
    if reasons & SUMMARY_REASONS:
        zones.append("summary")
    if reasons & TABLE_REASONS:
        zones.append("table")
    return ",".join(zones or ["metadata", "summary", "table"])


def build_p0_fallback_targets(config: ProjectConfig) -> pd.DataFrame:
    queue = _read_review_queue(config)
    if queue.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)
    p0 = queue[queue["priority"].astype(str) == "P0"].copy()
    if p0.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    rows: list[dict[str, object]] = []
    group_columns = ["source_pdf", "source_page", "form_type", "polling_station_no"]
    for keys, group in p0.groupby(group_columns, dropna=False):
        source_pdf, source_page, form_type, polling_station_no = keys
        reasons = sorted(set(group["reason"].dropna().astype(str)))
        reason_set = set(reasons)
        rows.append(
            {
                "priority": "P0",
                "source_pdf": source_pdf,
                "source_page": source_page,
                "form_type": form_type,
                "polling_station_no": polling_station_no,
                "row_count": len(group),
                "reasons": "|".join(reasons),
                "suggested_zones": _suggested_zones(reason_set),
                "suggested_fallback": "cell_crop_then_google_or_manual_review",
            }
        )
    return pd.DataFrame(rows, columns=TARGET_COLUMNS).sort_values(
        ["source_pdf", "source_page", "form_type"], kind="stable"
    )


def write_p0_fallback_targets(config: ProjectConfig) -> Path:
    config.ensure_output_dirs()
    output_path = (
        config.output("p0_fallback_targets")
        if "p0_fallback_targets" in config.outputs
        else config.path("processed_dir") / "p0_fallback_targets.csv"
    )
    build_p0_fallback_targets(config).to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build P0 OCR fallback target pages.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    args = parser.parse_args()
    print(write_p0_fallback_targets(load_config(args.config)))


if __name__ == "__main__":
    main()
