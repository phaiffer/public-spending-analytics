"""CLI entrypoint for local project utilities."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from gov_spending_analytics.ingestion.portal_transparencia import discover_raw_csv_files
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

    if args.command == "bootstrap-duckdb":
        database_path = bootstrap_duckdb(Path(config["duckdb"]["database_path"]))
        print(f"DuckDB database ready at: {database_path}")
        return

    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
