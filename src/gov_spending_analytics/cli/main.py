"""CLI entrypoint for local project utilities."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from gov_spending_analytics.ingestion.portal_transparencia import discover_raw_csv_files
from gov_spending_analytics.profiling.raw_csv import profile_raw_csv_file, select_raw_csv_file
from gov_spending_analytics.utils.config import load_project_config
from gov_spending_analytics.utils.duckdb_bootstrap import bootstrap_duckdb


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gov-spending",
        description="Local utilities for Brazilian federal public spending analytics.",
    )
    parser.add_argument(
        "--config",
        default="config/project.toml",
        help="Path to the project TOML configuration file.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("show-config", help="Print the resolved project configuration.")
    subparsers.add_parser("list-raw-files", help="List discovered raw CSV files.")

    profile_parser = subparsers.add_parser(
        "profile-raw-file",
        help="Profile one manually downloaded raw CSV file.",
    )
    profile_parser.add_argument(
        "--file",
        type=Path,
        help="Explicit path to the raw CSV file to profile.",
    )
    profile_parser.add_argument(
        "--pattern",
        help="Case-insensitive substring used to select one discovered raw CSV file.",
    )
    profile_parser.add_argument(
        "--output",
        type=Path,
        help="Output JSON profile path. Defaults to the configured profiling folder.",
    )
    profile_parser.add_argument(
        "--sample-size",
        type=int,
        default=20,
        help="Number of sample records to include in the profile output.",
    )
    profile_parser.add_argument(
        "--inference-rows",
        type=int,
        default=5_000,
        help="Number of rows used for type and null profiling.",
    )
    profile_parser.add_argument(
        "--null-threshold",
        type=float,
        default=0.8,
        help="Null ratio at or above which a column is marked as null-heavy.",
    )

    subparsers.add_parser(
        "bootstrap-duckdb",
        help="Create the local DuckDB database file if it does not already exist.",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = load_project_config(Path(args.config))

    if args.command == "show-config":
        print(json.dumps(config, indent=2, sort_keys=True))
        return

    if args.command == "list-raw-files":
        raw_files = discover_raw_csv_files(Path(config["paths"]["raw_data"]))
        for file_path in raw_files:
            print(file_path)
        if not raw_files:
            print("No raw CSV files found.")
        return

    if args.command == "profile-raw-file":
        raw_data_path = Path(config["paths"]["raw_data"])
        try:
            selected_file = select_raw_csv_file(raw_data_path, args.file, args.pattern)
        except (FileNotFoundError, ValueError) as exc:
            parser.error(str(exc))

        output_path = args.output
        if output_path is None:
            profiling_path = Path(config["paths"].get("profiling_artifacts", "profiling"))
            output_path = profiling_path / f"{selected_file.stem}_profile.json"

        try:
            profile_path = profile_raw_csv_file(
                file_path=selected_file,
                output_path=output_path,
                sample_size=args.sample_size,
                inference_rows=args.inference_rows,
                null_threshold=args.null_threshold,
            )
        except (OSError, ValueError, RuntimeError) as exc:
            parser.error(str(exc))

        print(f"Profile written to: {profile_path}")
        return

    if args.command == "bootstrap-duckdb":
        database_path = bootstrap_duckdb(Path(config["duckdb"]["database_path"]))
        print(f"DuckDB database ready at: {database_path}")
        return

    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
