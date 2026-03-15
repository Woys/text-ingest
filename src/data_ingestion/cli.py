"""Command-line entry point for mdi-run."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from data_ingestion.pipeline import run_to_jsonl, run_to_jsonl_with_full_text
from data_ingestion.registry import list_fetchers


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mdi-run",
        description="Stream normalized records from multiple APIs into a JSONL file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Sub-command for running the pipeline
    run_parser = subparsers.add_parser(
        "run",
        help="Run the data ingestion pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    run_parser.add_argument("--spec-file", required=True, metavar="PATH")
    run_parser.add_argument("--output-file", required=True, metavar="PATH")
    run_parser.add_argument("--full-text-output-file", metavar="PATH", default=None)
    run_parser.add_argument("--full-text-max-chars", type=int, default=200_000)
    run_parser.add_argument("--overwrite", action="store_true")
    run_parser.add_argument("--continue-on-error", action="store_true")
    run_parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    run_parser.add_argument("--start-date", default=None, metavar="YYYY-MM-DD")
    run_parser.add_argument("--end-date", default=None, metavar="YYYY-MM-DD")

    # Sub-command for listing fetchers
    subparsers.add_parser("list-fetchers", help="List available fetchers.")

    return parser


def main() -> None:
    args = _build_parser().parse_args()

    if args.command == "list-fetchers":
        display_aliases = {
            "hackernews": "hacker-news",
            "federalregister": "federal-register",
        }
        for fetcher in sorted(list_fetchers()):
            print(fetcher)
            alias = display_aliases.get(fetcher)
            if alias:
                print(alias)
        return

    os.environ["MDI_LOG_LEVEL"] = args.log_level.upper()

    spec_path = Path(args.spec_file)
    if not spec_path.is_file():
        print(f"error: spec file not found: {spec_path}", file=sys.stderr)
        sys.exit(1)

    with spec_path.open("r", encoding="utf-8") as fh:
        fetcher_specs = json.load(fh)

    date_overrides = {}
    if args.start_date is not None:
        date_overrides["start_date"] = args.start_date
    if args.end_date is not None:
        date_overrides["end_date"] = args.end_date

    if args.full_text_output_file:
        summary = run_to_jsonl_with_full_text(
            fetcher_specs=fetcher_specs,
            output_file=args.output_file,
            full_text_output_file=args.full_text_output_file,
            append=not args.overwrite,
            fail_fast=not args.continue_on_error,
            full_text_max_chars=args.full_text_max_chars,
            **date_overrides,
        )
    else:
        summary = run_to_jsonl(
            fetcher_specs=fetcher_specs,
            output_file=args.output_file,
            append=not args.overwrite,
            fail_fast=not args.continue_on_error,
            **date_overrides,
        )

    print(summary.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
