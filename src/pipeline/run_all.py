from __future__ import annotations

import argparse

from src.analysis.insights import build_insights
from src.ocr.extract import run_extraction
from src.pipeline.clean import clean_results
from src.pipeline.config import load_config
from src.pipeline.dashboard_prep import build_dashboard_dataset
from src.pipeline.manifest import load_manifest, missing_required_entries, write_manifest_status
from src.pipeline.validate import validate_results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the complete Election 2026 pipeline.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    parser.add_argument("--limit-pages", type=int, default=None)
    parser.add_argument("--max-files", type=int, default=None)
    parser.add_argument(
        "--skip-ocr",
        action="store_true",
        help="Skip OCR and rebuild cleaned, validation, analysis, and dashboard outputs.",
    )
    parser.add_argument("--overwrite-ocr", action="store_true")
    args = parser.parse_args()

    config = load_config(args.config)
    config.ensure_output_dirs()

    entries = load_manifest(config)
    manifest_path = write_manifest_status(config, entries)
    missing = missing_required_entries(entries)
    if args.skip_ocr:
        print("Skipping OCR by request.")
    elif missing:
        print("Skipping OCR because required PDFs are missing:")
        for entry in missing:
            print(f"- {entry.file_path}")
        print(f"Manifest status written to {manifest_path}")
    else:
        for output in run_extraction(
            config,
            limit_pages=args.limit_pages,
            max_files=args.max_files,
            skip_existing=not args.overwrite_ocr,
        ):
            print(f"OCR parsed output: {output}")

    results_path, summary_path = clean_results(config)
    validation_csv, validation_md = validate_results(config)
    dashboard_paths = build_dashboard_dataset(config)
    insights_path = build_insights(config)

    print(f"Cleaned results: {results_path}")
    print(f"Station summary: {summary_path}")
    print(f"Validation report: {validation_csv}")
    print(f"Validation markdown: {validation_md}")
    print(f"Dashboard datasets: {dashboard_paths[0]}, {dashboard_paths[1]}")
    print(f"Insights report: {insights_path}")


if __name__ == "__main__":
    main()
