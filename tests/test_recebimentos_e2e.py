"""End-to-end run of profiling, staging, and dbt build over the synthetic sample."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from gov_spending_analytics.profiling.raw_csv import profile_raw_csv_file
from gov_spending_analytics.staging.portal_transparencia_recebimentos import (
    RecebimentosStagingResult,
    stage_recebimentos_recursos_por_favorecido_csv,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "tests" / "fixtures"
SAMPLE_CSV = FIXTURES / "recebimentos_sample" / "202601_RecebimentosRecursosPorFavorecido.csv"
SAMPLE_MANIFEST = FIXTURES / "source_manifest.toml"
SAMPLE_ROW_COUNT = 19


@pytest.fixture(scope="module")
def staged_sample(tmp_path_factory: pytest.TempPathFactory) -> RecebimentosStagingResult:
    work_dir = tmp_path_factory.mktemp("recebimentos_e2e")
    raw_file = work_dir / "raw" / SAMPLE_CSV.name
    raw_file.parent.mkdir()
    shutil.copyfile(SAMPLE_CSV, raw_file)

    profile_path = profile_raw_csv_file(
        file_path=raw_file,
        output_path=work_dir / "profiling" / f"{raw_file.stem}_profile.json",
    )
    return stage_recebimentos_recursos_por_favorecido_csv(
        file_path=raw_file,
        profile_path=profile_path,
        output_path=work_dir / "staging" / f"{raw_file.stem}.parquet",
        manifest_path=SAMPLE_MANIFEST,
    )


def find_dbt_executable() -> str | None:
    candidate = Path(sys.executable).parent / "dbt"
    if candidate.exists():
        return str(candidate)
    return shutil.which("dbt")


def test_staging_preserves_every_sample_row(staged_sample: RecebimentosStagingResult) -> None:
    staged = pd.read_parquet(staged_sample.output_path)

    assert staged_sample.row_count == SAMPLE_ROW_COUNT
    assert len(staged) == SAMPLE_ROW_COUNT
    assert staged["source_row_number"].tolist() == list(range(1, SAMPLE_ROW_COUNT + 1))
    assert set(staged["source_file_sha256"]) == {staged_sample.source_file_sha256}
    assert (staged["amount_received_brl"] < 0).sum() == 2
    assert "Exterior" in set(staged["beneficiary_municipality_name"])


def require_dbt() -> str:
    dbt = find_dbt_executable()
    if dbt is None:
        if os.environ.get("CI"):
            pytest.fail("dbt CLI is required in CI")
        pytest.skip("dbt CLI is not installed; install the dev extra to run this test")
    return dbt


def run_dbt_build(dbt: str, work_dir: Path, staged_parquet: str) -> Path:
    """Run dbt build into work_dir/sample.duckdb, assert it is clean, return the file."""
    database_path = work_dir / "sample.duckdb"
    (work_dir / "profiles.yml").write_text(
        "gov_spending_analytics:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: duckdb\n"
        f"      path: {json.dumps(str(database_path))}\n"
        "      threads: 4\n",
        encoding="utf-8",
    )
    dbt_vars = {"recebimentos_staged_parquet": staged_parquet}
    completed = subprocess.run(
        [
            dbt,
            "build",
            "--profiles-dir",
            str(work_dir),
            "--target-path",
            str(work_dir / "target"),
            "--log-path",
            str(work_dir / "logs"),
            "--vars",
            json.dumps(dbt_vars),
        ],
        cwd=REPO_ROOT / "dbt",
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout[-4000:] + completed.stderr[-4000:]
    assert "WARN=0 ERROR=0" in completed.stdout
    return database_path


def test_dbt_build_over_sample(
    staged_sample: RecebimentosStagingResult,
    tmp_path: Path,
) -> None:
    dbt = require_dbt()
    database_path = run_dbt_build(dbt, tmp_path, str(staged_sample.output_path))

    with duckdb.connect(str(database_path), read_only=True) as connection:
        kinds = dict(
            connection.execute(
                "select beneficiary_identifier_kind, count(*) "
                "from main_staging.stg_portal_transparencia__recebimentos_recursos_por_favorecido "
                "group by 1"
            ).fetchall()
        )
        mart_rows = connection.execute(
            "select count(*) from main_marts.mart_recebimentos_by_beneficiary_month"
        ).fetchone()[0]
        multi_name_ids = connection.execute(
            "select beneficiary_id, beneficiary_identifier_kind "
            "from main_marts.mart_recebimentos_by_beneficiary_month "
            "group by 1, 2 having count(distinct beneficiary_name) > 1"
        ).fetchall()

    assert kinds == {
        "cnpj": 5,
        "cpf_masked": 4,
        "foreign_code": 2,
        "placeholder": 3,
        "treasury_operation": 1,
        "treasury_management_unit": 1,
        "management_unit": 1,
        "pf_code": 1,
        "unregistered_code": 1,
    }
    assert mart_rows == 17
    assert multi_name_ids == [("***.111.222-**", "cpf_masked")]


INCREMENTAL_MODELS = (
    "main_intermediate.int_recebimentos_recursos_por_favorecido_monthly",
    "main_marts.mart_recebimentos_by_beneficiary_month",
    "main_marts.mart_recebimentos_monthly_summary",
)


def write_month(staged: pd.DataFrame, month: str, directory: Path, **overrides) -> None:
    """Write the staged sample as another month's staged file, as the sync would."""
    file_name = f"{month.replace('-', '')}_RecebimentosRecursosPorFavorecido"
    frame = staged.assign(launch_month=month, source_file_name=f"{file_name}.csv", **overrides)
    frame.to_parquet(directory / f"{file_name}.parquet", index=False)


def built_at_by_month(database_path: Path) -> dict[str, dict[int, object]]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        return {
            model: dict(
                connection.execute(
                    f"select launch_month_key, max(built_at) from {model} group by 1"
                ).fetchall()
            )
            for model in INCREMENTAL_MODELS
        }


def test_incremental_build_rebuilds_only_changed_months(
    staged_sample: RecebimentosStagingResult,
    tmp_path: Path,
) -> None:
    dbt = require_dbt()
    staged = pd.read_parquet(staged_sample.output_path)
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    write_month(staged, "2026-01", staging_dir)
    write_month(staged, "2026-02", staging_dir)
    glob = str(staging_dir / "*.parquet")

    database_path = run_dbt_build(dbt, tmp_path, glob)
    first = built_at_by_month(database_path)
    assert all(set(months) == {202601, 202602} for months in first.values())

    run_dbt_build(dbt, tmp_path, glob)
    assert built_at_by_month(database_path) == first, "an unchanged month was rebuilt"

    # The Portal republishes February with one amount doubled.
    amounts = staged["amount_received_brl"].copy()
    amounts.iloc[0] *= 2
    write_month(
        staged, "2026-02", staging_dir, source_file_sha256="0" * 64, amount_received_brl=amounts
    )
    run_dbt_build(dbt, tmp_path, glob)
    second = built_at_by_month(database_path)
    for model in INCREMENTAL_MODELS:
        assert second[model][202601] == first[model][202601], model
        assert second[model][202602] > first[model][202602], model

    with duckdb.connect(str(database_path), read_only=True) as connection:
        totals = dict(
            connection.execute(
                "select launch_month_key, sum(total_amount_received_brl) "
                "from main_marts.mart_recebimentos_monthly_summary group by 1"
            ).fetchall()
        )
    assert totals[202602] - totals[202601] == staged["amount_received_brl"].iloc[0]
