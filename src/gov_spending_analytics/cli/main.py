"""CLI entrypoint for local project utilities."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from gov_spending_analytics.ingestion.portal_transparencia import discover_raw_csv_files
from gov_spending_analytics.profiling.raw_csv import profile_raw_csv_file, select_raw_csv_file
from gov_spending_analytics.utils.config import load_project_config
from gov_spending_analytics.utils.duckdb_bootstrap import bootstrap_duckdb

EXPERIMENTAL_COMMANDS = {"stage-despesas-file", "ingest-despesas-documentos"}


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
        help="[experimental] Stage one profiled Portal da Transparencia despesas CSV as Parquet.",
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
    recebimentos_parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("config/source_manifest.toml"),
        help=(
            "Source manifest that registers the raw file version by SHA-256. "
            "Defaults to config/source_manifest.toml."
        ),
    )

    sync_parser = subparsers.add_parser(
        "sync-recebimentos",
        help=(
            "Download new or republished monthly Recebimentos files from the Portal, "
            "register each version, and stage it."
        ),
    )
    sync_range = sync_parser.add_mutually_exclusive_group()
    sync_range.add_argument(
        "--all",
        action="store_true",
        help="Check every published month, from 2013-12 to the current month.",
    )
    sync_range.add_argument(
        "--since",
        help="First month to check, as YYYY-MM. Without --since or --all, sync checks "
        "every month it already tracks plus newer ones (the last 12 months on a first run).",
    )
    sync_parser.add_argument(
        "--until",
        help="Last month to check, as YYYY-MM. Defaults to the current month.",
    )
    sync_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only ask the server what changed; download nothing.",
    )
    sync_parser.add_argument(
        "--keep-csv",
        action="store_true",
        help="Keep the extracted CSV next to the downloaded archive after staging.",
    )
    sync_parser.add_argument(
        "--dbt-build",
        action="store_true",
        help="Run dbt build when at least one month was staged.",
    )

    despesas_documentos_parser = subparsers.add_parser(
        "ingest-despesas-documentos",
        help="[experimental] Ingest raw Portal da Transparencia despesas/documentos API pages.",
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
    if args.command in EXPERIMENTAL_COMMANDS:
        print(
            f"Warning: {args.command} is experimental and not part of the implemented "
            "case study; see the README.",
            file=sys.stderr,
        )
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

        manifest_path = args.manifest
        if not manifest_path.is_absolute():
            manifest_path = Path.cwd() / manifest_path

        try:
            result = stage_recebimentos_recursos_por_favorecido_csv(
                file_path=selected_file,
                profile_path=profile_path,
                output_path=output_path,
                manifest_path=manifest_path,
            )
        except (OSError, ValueError, RuntimeError) as exc:
            parser.error(str(exc))

        print(f"Staged Parquet written to: {result.output_path}")
        print(f"Rows staged: {result.row_count}")
        print(f"Source file SHA-256: {result.source_file_sha256}")
        print(f"Source family: {result.source_family}")
        print("Column mapping:")
        for source_column, staged_column in result.column_mapping.items():
            print(f"- {source_column}: {staged_column}")
        return

    if args.command == "sync-recebimentos":
        run_sync_recebimentos(args, parser)
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


def run_sync_recebimentos(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Sync the monthly Recebimentos files and optionally rebuild the dbt project."""
    from gov_spending_analytics.ingestion.portal_sync import (
        SyncPaths,
        count_by_status,
        current_month,
        load_remote_state,
        select_months,
        sync_recebimentos,
    )
    from gov_spending_analytics.utils.dbt_runner import run_dbt_build

    paths = SyncPaths()
    try:
        months = select_months(
            include_all=args.all,
            since=args.since,
            until=args.until,
            remote_state=load_remote_state(paths.remote_state_path),
            today_month=current_month(),
        )
    except ValueError as exc:
        parser.error(str(exc))

    print(f"Checking {len(months)} months: {months[0]} to {months[-1]}")
    results = sync_recebimentos(
        months=months,
        paths=paths,
        keep_csv=args.keep_csv,
        dry_run=args.dry_run,
        report=lambda result: print(
            f"{result.reference_month} {result.status:<14} {result.detail}", flush=True
        ),
    )
    counts = count_by_status(results)
    print("Summary: " + ", ".join(f"{status}={count}" for status, count in sorted(counts.items())))

    if args.dbt_build and counts.get("staged"):
        print("Running dbt build")
        if run_dbt_build() != 0:
            sys.exit("dbt build failed")
    if counts.get("failed"):
        sys.exit(f"{counts['failed']} month(s) failed; they are retried on the next run")


if __name__ == "__main__":
    main()
