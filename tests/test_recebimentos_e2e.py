"""End-to-end run of profiling, staging, and dbt build over the synthetic sample."""

from __future__ import annotations

import json
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
SAMPLE_ROW_COUNT = 16


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


def test_dbt_build_over_sample(
    staged_sample: RecebimentosStagingResult,
    tmp_path: Path,
) -> None:
    dbt = find_dbt_executable()
    if dbt is None:
        pytest.skip("dbt CLI is not installed; install the dev extra to run this test")

    database_path = tmp_path / "sample.duckdb"
    (tmp_path / "profiles.yml").write_text(
        "gov_spending_analytics:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: duckdb\n"
        f"      path: {json.dumps(str(database_path))}\n"
        "      threads: 4\n",
        encoding="utf-8",
    )
    dbt_vars = {
        "recebimentos_staged_parquet": str(staged_sample.output_path),
        "recebimentos_expected_row_count": SAMPLE_ROW_COUNT,
    }
    completed = subprocess.run(
        [
            dbt,
            "build",
            "--profiles-dir",
            str(tmp_path),
            "--target-path",
            str(tmp_path / "target"),
            "--log-path",
            str(tmp_path / "logs"),
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
        "other_code": 2,
    }
    assert mart_rows == 14
    assert multi_name_ids == [("***.111.222-**", "cpf_masked")]
