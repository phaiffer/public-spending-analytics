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

    stage_parser = subparsers.add_parser(
        "stage-despesas-file",
        help="Stage one profiled Portal da Transparencia despesas CSV as Parquet.",
    )
    stage_parser.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Path to the raw CSV file to stage.",
    )
    stage_parser.add_argument(
        "--profile",
        type=Path,
        help="Path to the JSON profile artifact. Defaults to profiling/<file stem>_profile.json.",
    )
    stage_parser.add_argument(
        "--output",
        type=Path,
        help="Output Parquet path. Defaults under data/staging/portal_transparencia/despesas/.",
    )

    recebimentos_parser = subparsers.add_parser(
        "stage-recebimentos-favorecido-file",
        help="Stage the profiled Recebimentos de Recursos por Favorecido CSV as Parquet.",
    )
    recebimentos_parser.add_argument(
        "--file",
        type=Path,
        default=Path("data/raw/202601_RecebimentosRecursosPorFavorecido.csv"),
        help=(
            "Path to the raw CSV file. Defaults to "
            "data/raw/202601_RecebimentosRecursosPorFavorecido.csv."
        ),
    )
    recebimentos_parser.add_argument(
        "--profile",
        type=Path,
        default=Path("profiling/202601_RecebimentosRecursosPorFavorecido_profile.json"),
        help=(
            "Path to the JSON profile artifact. Defaults to "
            "profiling/202601_RecebimentosRecursosPorFavorecido_profile.json."
        ),
    )
    recebimentos_parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Output Parquet path. Defaults under "
            "data/staging/portal_transparencia/recebimentos_recursos_por_favorecido/."
        ),
    )

    despesas_documentos_parser = subparsers.add_parser(
        "ingest-despesas-documentos",
        help="Ingest raw Portal da Transparencia despesas/documentos API pages.",
    )
    despesas_documentos_parser.add_argument(
        "--data-emissao",
        required=True,
        help="Required issue date in DD/MM/YYYY format. Example: 02/01/2025.",
    )
    despesas_documentos_parser.add_argument(
        "--fase",
        type=int,
        required=True,
        choices=(1, 2, 3),
        help="Required spending phase code: 1=empenho, 2=liquidacao, 3=pagamento.",
    )
    despesas_documentos_parser.add_argument(
        "--unidade-gestora",
        help="Optional Unidade Gestora emitente code. Required when --gestao is omitted.",
    )
    despesas_documentos_parser.add_argument(
        "--gestao",
        help="Optional Gestao code. Required when --unidade-gestora is omitted.",
    )
    despesas_documentos_parser.add_argument(
        "--pagina-inicial",
        type=int,
        default=1,
        help="First API page to request. Defaults to 1.",
    )
    despesas_documentos_parser.add_argument(
        "--max-paginas",
        type=int,
        help="Optional maximum number of non-empty pages to persist.",
    )
    despesas_documentos_parser.add_argument(
        "--output-dir",
        type=Path,
        help=(
            "Raw JSON output directory. Defaults under "
            "data/raw/portal_transparencia_api/despesas_documentos/."
        ),
    )
    despesas_documentos_parser.add_argument(
        "--api-key",
        help=(
            "Portal da Transparencia API key. Prefer the configured environment "
            "variable instead of passing secrets on the command line."
        ),
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

    if args.command == "stage-despesas-file":
        try:
            from gov_spending_analytics.staging.portal_transparencia_despesas import (
                stage_profiled_despesas_csv,
            )
        except ModuleNotFoundError as exc:
            parser.error(
                f"Missing Python dependency for staging: {exc.name}. "
                'Install project dependencies with: python -m pip install -e ".[dev]"'
            )

        selected_file = args.file
        if not selected_file.is_absolute():
            selected_file = Path.cwd() / selected_file

        profile_path = args.profile
        if profile_path is None:
            profiling_path = Path(config["paths"].get("profiling_artifacts", "profiling"))
            profile_path = profiling_path / f"{selected_file.stem}_profile.json"
        elif not profile_path.is_absolute():
            profile_path = Path.cwd() / profile_path

        output_path = args.output
        if output_path is not None and not output_path.is_absolute():
            output_path = Path.cwd() / output_path

        try:
            result = stage_profiled_despesas_csv(
                file_path=selected_file,
                profile_path=profile_path,
                output_path=output_path,
            )
        except (OSError, ValueError, RuntimeError) as exc:
            parser.error(str(exc))

        print(f"Staged Parquet written to: {result.output_path}")
        print(f"Rows staged: {result.row_count}")
        print(f"Source family: {result.source_family}")
        print(f"Spending stage: {result.spending_stage}")
        print("Canonical mapping:")
        for canonical_name, source_column in sorted(result.canonical_mapping.items()):
            print(f"- {canonical_name}: {source_column}")
        return

    if args.command == "stage-recebimentos-favorecido-file":
        try:
            from gov_spending_analytics.staging.portal_transparencia_recebimentos import (
                stage_recebimentos_recursos_por_favorecido_csv,
            )
        except ModuleNotFoundError as exc:
            parser.error(
                f"Missing Python dependency for staging: {exc.name}. "
                'Install project dependencies with: python -m pip install -e ".[dev]"'
            )

        selected_file = args.file
        if not selected_file.is_absolute():
            selected_file = Path.cwd() / selected_file

        profile_path = args.profile
        if not profile_path.is_absolute():
            profile_path = Path.cwd() / profile_path

        output_path = args.output
        if output_path is not None and not output_path.is_absolute():
            output_path = Path.cwd() / output_path

        try:
            result = stage_recebimentos_recursos_por_favorecido_csv(
                file_path=selected_file,
                profile_path=profile_path,
                output_path=output_path,
            )
        except (OSError, ValueError, RuntimeError) as exc:
            parser.error(str(exc))

        print(f"Staged Parquet written to: {result.output_path}")
        print(f"Rows staged: {result.row_count}")
        print(f"Source family: {result.source_family}")
        print("Column mapping:")
        for source_column, staged_column in result.column_mapping.items():
            print(f"- {source_column}: {staged_column}")
        return

    if args.command == "ingest-despesas-documentos":
        try:
            from gov_spending_analytics.ingestion.portal_transparencia_api import (
                build_api_client_from_config,
                build_despesas_documentos_request,
                default_despesas_documentos_output_dir,
                ingest_despesas_documentos,
                resolve_api_key,
            )
        except ModuleNotFoundError as exc:
            parser.error(
                f"Missing Python dependency for API ingestion: {exc.name}. "
                'Install project dependencies with: python -m pip install -e ".[dev]"'
            )

        try:
            request_params = build_despesas_documentos_request(
                data_emissao=args.data_emissao,
                fase=args.fase,
                unidade_gestora=args.unidade_gestora,
                gestao=args.gestao,
                pagina_inicial=args.pagina_inicial,
                max_paginas=args.max_paginas,
            )
        except ValueError as exc:
            parser.error(str(exc))

        output_dir = args.output_dir
        if output_dir is None:
            output_dir = default_despesas_documentos_output_dir(
                raw_data_path=Path(config["paths"]["raw_data"]),
                request_params=request_params,
            )
        elif not output_dir.is_absolute():
            output_dir = Path.cwd() / output_dir

        try:
            api_key = resolve_api_key(config=config, explicit_api_key=args.api_key)
            client = build_api_client_from_config(config=config, api_key=api_key)
            result = ingest_despesas_documentos(
                client=client,
                request_params=request_params,
                output_dir=output_dir,
            )
        except (OSError, ValueError, RuntimeError) as exc:
            parser.error(str(exc))

        print(f"Raw API responses written under: {result.output_dir}")
        print(f"Manifest written to: {result.manifest_path}")
        print(f"Source endpoint: {result.source_endpoint}")
        print(f"Pages persisted: {result.page_count}")
        print(f"Records fetched: {result.total_records}")
        return

    if args.command == "bootstrap-duckdb":
        database_path = bootstrap_duckdb(Path(config["duckdb"]["database_path"]))
        print(f"DuckDB database ready at: {database_path}")
        return

    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
