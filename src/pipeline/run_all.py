from __future__ import annotations

import argparse

from src.analysis.insights import build_insights
from src.ocr.digit_crops import write_digit_crop_manifest
from src.ocr.extract import run_extraction, select_manifest_entries
from src.pipeline.clean import clean_results
from src.pipeline.config import load_config
from src.pipeline.dashboard_prep import build_dashboard_dataset
from src.pipeline.final_export import export_final_schema
from src.pipeline.manifest import load_manifest, missing_required_entries, write_manifest_status
from src.pipeline.ocr_progress import write_ocr_progress
from src.pipeline.validate import validate_results
from src.quality.evaluate_accuracy import write_accuracy_outputs
from src.quality.master_match import write_master_match_report
from src.quality.p0_fallback_targets import write_p0_fallback_targets
from src.quality.review_queue import write_review_queue


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the complete Election 2026 pipeline.")
    parser.add_argument("--config", default="configs/chaiyaphum_2.yaml")
    parser.add_argument("--limit-pages", type=int, default=None)
    parser.add_argument("--max-files", type=int, default=None)
    parser.add_argument(
        "--start-index",
        type=int,
        default=None,
        help="1-based manifest row to start OCR from.",
    )
    parser.add_argument(
        "--end-index",
        type=int,
        default=None,
        help="1-based manifest row to stop OCR at, inclusive.",
    )
    parser.add_argument(
        "--form-type",
        action="append",
        default=None,
        help="Restrict OCR to one or more form types. Repeat or comma-separate values.",
    )
    parser.add_argument(
        "--file-contains",
        default=None,
        help="Restrict OCR to manifest files whose path contains this text.",
    )
    parser.add_argument(
        "--skip-ocr",
        action="store_true",
        help="Skip OCR and rebuild cleaned, validation, analysis, and dashboard outputs.",
    )
    parser.add_argument("--overwrite-ocr", action="store_true")
    parser.add_argument(
        "--prepare-digit-crops",
        action="store_true",
        help="Create digit-only crop images for P0 missing vote rows.",
    )
    parser.add_argument(
        "--max-digit-crop-targets",
        type=int,
        default=None,
        help="Limit P0 digit crop targets when --prepare-digit-crops is used.",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    config.ensure_output_dirs()

    entries = load_manifest(config)
    manifest_path = write_manifest_status(config, entries)
    selected_entries = select_manifest_entries(
        entries,
        start_index=args.start_index,
        end_index=args.end_index,
        form_types=args.form_type,
        file_contains=args.file_contains,
    )
    missing = missing_required_entries(selected_entries)
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
            start_index=args.start_index,
            end_index=args.end_index,
            form_types=args.form_type,
            file_contains=args.file_contains,
            skip_existing=not args.overwrite_ocr,
        ):
            print(f"OCR parsed output: {output}")

    results_path, summary_path = clean_results(config)
    validation_csv, validation_md = validate_results(config)
    dashboard_paths = build_dashboard_dataset(config)
    final_paths = export_final_schema(config)
    progress_path = write_ocr_progress(config)
    master_match_path = write_master_match_report(config)
    review_queue_path = write_review_queue(config)
    p0_fallback_targets_path = write_p0_fallback_targets(config)
    digit_crops_path = None
    if args.prepare_digit_crops:
        digit_crops_path = write_digit_crop_manifest(
            config,
            max_targets=args.max_digit_crop_targets,
        )
    accuracy_paths = write_accuracy_outputs(config)
    insights_path = build_insights(config)

    print(f"Cleaned results: {results_path}")
    print(f"Station summary: {summary_path}")
    print(f"Validation report: {validation_csv}")
    print(f"Validation markdown: {validation_md}")
    print(f"Dashboard datasets: {dashboard_paths[0]}, {dashboard_paths[1]}")
    print(f"Final CSV schema: {final_paths[0]}, {final_paths[1]}")
    print(f"OCR progress: {progress_path}")
    print(f"Master match report: {master_match_path}")
    print(f"Review queue: {review_queue_path}")
    print(f"P0 fallback targets: {p0_fallback_targets_path}")
    if digit_crops_path is not None:
        print(f"P0 digit crop manifest: {digit_crops_path}")
    print(f"Accuracy report: {accuracy_paths[0]}, {accuracy_paths[1]}, {accuracy_paths[2]}")
    print(f"Insights report: {insights_path}")


if __name__ == "__main__":
    main()
