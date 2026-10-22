"""Dashboard formatting, queries over a synthetic DuckDB, and a page smoke test."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from gov_spending_analytics.dashboard import queries
from gov_spending_analytics.dashboard.formatting import (
    amount_scale,
    format_brl,
    format_cnpj,
    format_number,
    month_label,
)

APP_PATH = (
    Path(__file__).resolve().parents[1] / "src" / "gov_spending_analytics" / "dashboard" / "app.py"
)

# (month, body id, body name, kind, amount, records, negatives)
SUMMARY_ROWS = [
    (202512, "36000", "Ministério da Saúde", "cnpj", 1000.0, 3, 0),
    (202512, "26000", "Ministério da Educação", "cpf_masked", 400.0, 2, 1),
    (202601, "36000", "Ministério da Saúde", "cnpj", 2500.0, 4, 0),
    (202601, "26000", "Ministério da Educação e Cultura", "placeholder", 300.0, 1, 0),
    (202602, "36000", "Ministério da Saúde", "foreign_code", 50.0, 1, 0),
]
# (month, id, kind, name, amount)
BENEFICIARY_ROWS = [
    (202512, "11222333000181", "cnpj", "EMPRESA ALFA", 1000.0),
    (202601, "11222333000181", "cnpj", "EMPRESA ALFA RENOMEADA", 2000.0),
    (202601, "44555666000199", "cnpj", "EMPRESA BETA", 500.0),
    (202601, "***.111.222-**", "cpf_masked", "PESSOA FICTICIA", 400.0),
    (202602, "EXFICTCOR", "foreign_code", "FICTITIOUS CORP", 50.0),
]


def build_database(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("create schema main_marts")
    con.execute(
        """
        create table main_marts.mart_recebimentos_monthly_summary (
            launch_month_key integer, launch_month_start_date date,
            superior_government_body_id varchar, superior_government_body_name varchar,
            beneficiary_identifier_kind varchar, total_amount_received_brl decimal(18, 4),
            total_record_count bigint, negative_amount_record_count bigint
        )
        """
    )
    for month, body_id, body_name, kind, amount, records, negatives in SUMMARY_ROWS:
        con.execute(
            "insert into main_marts.mart_recebimentos_monthly_summary values "
            "(?, strptime(? || '01', '%Y%m%d'), ?, ?, ?, ?, ?, ?)",
            [month, str(month), body_id, body_name, kind, amount, records, negatives],
        )
    con.execute(
        """
        create table main_marts.mart_recebimentos_by_beneficiary_month (
            launch_month_key integer, beneficiary_id varchar,
            beneficiary_identifier_kind varchar, beneficiary_name varchar,
            total_amount_received_brl decimal(18, 4)
        )
        """
    )
    con.executemany(
        "insert into main_marts.mart_recebimentos_by_beneficiary_month values (?, ?, ?, ?, ?)",
        BENEFICIARY_ROWS,
    )


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    build_database(connection)
    return connection


def test_formatting() -> None:
    assert format_number(1234567) == "1.234.567"
    assert format_brl(1_234_567_890) == "R$ 1,23 bi"
    assert format_brl(-45.25) == "-R$ 45,25"
    assert format_brl(980.1) == "R$ 980,10"
    assert amount_scale(2_500_000) == (1e6, "R$ mi")
    assert format_cnpj("11222333000181") == "11.222.333/0001-81"
    assert format_cnpj("EXFICTCOR") == "EXFICTCOR"
    assert month_label(202601) == "jan/2026"


def test_period_and_kind_filters(con: duckdb.DuckDBPyConnection) -> None:
    january = queries.DashboardFilters(202601, 202601)
    assert queries.headline_totals(con, january) == {
        "total_amount_brl": 2800.0,
        "record_count": 5,
        "negative_record_count": 0,
        "month_count": 1,
    }

    no_placeholders = queries.DashboardFilters(
        202512, 202602, identifier_kinds=("cnpj", "cpf_masked", "foreign_code")
    )
    monthly = queries.monthly_totals(con, no_placeholders)
    assert monthly["total_amount_brl"].tolist() == [1400.0, 2500.0, 50.0]


def test_body_filter_and_latest_body_name(con: duckdb.DuckDBPyConnection) -> None:
    bodies = queries.superior_bodies(con)
    names = dict(zip(bodies["body_id"], bodies["body_name"], strict=True))
    assert names["26000"] == "Ministério da Educação e Cultura", "latest published name"

    education = queries.DashboardFilters(202512, 202602, superior_body_ids=("26000",))
    ranking = queries.body_ranking(con, education)
    assert ranking["total_amount_brl"].tolist() == [700.0]


def test_organization_ranking_never_lists_people(con: duckdb.DuckDBPyConnection) -> None:
    everything = queries.DashboardFilters(202512, 202602)
    organizations = queries.top_organizations(con, everything)

    assert organizations["beneficiary_id"].tolist() == [
        "11222333000181",
        "44555666000199",
        "EXFICTCOR",
    ]
    assert organizations["beneficiary_name"].iloc[0] == "EMPRESA ALFA RENOMEADA"
    assert "cpf_masked" not in set(organizations["identifier_kind"])

    people_only = queries.DashboardFilters(202512, 202602, identifier_kinds=("cpf_masked",))
    assert queries.top_organizations(con, people_only).empty


def test_dashboard_page_renders(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("streamlit")
    from streamlit.testing.v1 import AppTest

    database_path = tmp_path / "dashboard.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        build_database(connection)
    monkeypatch.setenv("GOV_SPENDING_DUCKDB", str(database_path))
    monkeypatch.setenv("GOV_SPENDING_SYNC_MANIFEST", str(tmp_path / "missing.toml"))

    page = AppTest.from_file(str(APP_PATH), default_timeout=60).run()

    assert not page.exception
    assert page.title[0].value == "Recebimentos de recursos do Governo Federal"
    metrics = {metric.label: metric.value for metric in page.metric}
    assert metrics["Total recebido"] == "R$ 4,25 mil"
    assert metrics["Meses com dados"] == "3"
