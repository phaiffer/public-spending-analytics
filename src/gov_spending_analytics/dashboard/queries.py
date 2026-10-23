"""Read-only DuckDB queries behind the Streamlit dashboard.

Charts and KPIs read mart_recebimentos_monthly_summary, which stays small over the
full history. Only the organization ranking reads the beneficiary-level mart, and
it lists CNPJ and foreign-code beneficiaries only: masked CPFs are people, and the
dashboard does not rank individuals.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from gov_spending_analytics.ingestion.portal_sync import (
    DEFAULT_SYNC_MANIFEST_PATH,
    latest_entries_by_month,
)

SUMMARY_TABLE = "main_marts.mart_recebimentos_monthly_summary"
BENEFICIARY_TABLE = "main_marts.mart_recebimentos_by_beneficiary_month"
IDENTIFIER_KINDS = (
    "cnpj",
    "cpf_masked",
    "foreign_code",
    "placeholder",
    "treasury_payment",
    "intergovernmental_transfer",
    "treasury_unclassified",
    "other_code",
    "treasury_operation",
    "treasury_management_unit",
)
# Treasury cash management (public debt, bond issuance, Treasury and INSS transfers)
# and payments to the Treasury's own management units move trillions inside the
# government, so totals leave them out unless the viewer selects them.
INTERNAL_TREASURY_KINDS = ("treasury_operation", "treasury_management_unit")
DEFAULT_IDENTIFIER_KINDS = tuple(
    kind for kind in IDENTIFIER_KINDS if kind not in INTERNAL_TREASURY_KINDS
)
ORGANIZATION_KINDS = ("cnpj", "foreign_code")
ORGANIZATION_COLUMNS = [
    "beneficiary_id",
    "beneficiary_name",
    "identifier_kind",
    "total_amount_brl",
    "month_count",
]


@dataclass(frozen=True)
class DashboardFilters:
    """Period and dimension filters shared by every dashboard query."""

    start_month_key: int
    end_month_key: int
    superior_body_ids: tuple[str, ...] = ()
    identifier_kinds: tuple[str, ...] = DEFAULT_IDENTIFIER_KINDS


@dataclass(frozen=True)
class SourceFreshness:
    """When the local data was last synced, and which months were still open."""

    last_retrieved_at: str | None
    open_month_keys: frozenset[int]


def placeholders(values: tuple[str, ...]) -> str:
    return ", ".join("?" for _ in values)


def summary_filter(filters: DashboardFilters) -> tuple[str, list]:
    """SQL predicate and parameters for the summary mart."""
    clauses = [
        "launch_month_key between ? and ?",
        f"beneficiary_identifier_kind in ({placeholders(filters.identifier_kinds)})",
    ]
    params: list = [filters.start_month_key, filters.end_month_key, *filters.identifier_kinds]
    if filters.superior_body_ids:
        bodies = placeholders(filters.superior_body_ids)
        clauses.append(f"superior_government_body_id in ({bodies})")
        params.extend(filters.superior_body_ids)
    return " and ".join(clauses), params


def available_month_keys(con: duckdb.DuckDBPyConnection) -> list[int]:
    rows = con.execute(
        f"select distinct launch_month_key from {SUMMARY_TABLE} order by 1"
    ).fetchall()
    return [row[0] for row in rows]


def superior_bodies(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Every superior body with the most recent name it was published under."""
    return con.execute(
        f"""
        select
            superior_government_body_id as body_id,
            arg_max(superior_government_body_name, launch_month_key) as body_name
        from {SUMMARY_TABLE}
        group by 1
        order by 2
        """
    ).df()


def headline_totals(con: duckdb.DuckDBPyConnection, filters: DashboardFilters) -> dict:
    where, params = summary_filter(filters)
    row = con.execute(
        f"""
        select
            coalesce(sum(total_amount_received_brl), 0),
            coalesce(sum(total_record_count), 0),
            coalesce(sum(negative_amount_record_count), 0),
            count(distinct launch_month_key)
        from {SUMMARY_TABLE}
        where {where}
        """,
        params,
    ).fetchone()
    return {
        "total_amount_brl": float(row[0]),
        "record_count": int(row[1]),
        "negative_record_count": int(row[2]),
        "month_count": int(row[3]),
    }


def monthly_totals(con: duckdb.DuckDBPyConnection, filters: DashboardFilters) -> pd.DataFrame:
    where, params = summary_filter(filters)
    return con.execute(
        f"""
        select
            launch_month_key,
            launch_month_start_date,
            sum(total_amount_received_brl)::double as total_amount_brl,
            sum(total_record_count) as record_count,
            sum(negative_amount_record_count) as negative_record_count
        from {SUMMARY_TABLE}
        where {where}
        group by 1, 2
        order by 1
        """,
        params,
    ).df()


def body_ranking(
    con: duckdb.DuckDBPyConnection, filters: DashboardFilters, limit: int = 10
) -> pd.DataFrame:
    where, params = summary_filter(filters)
    return con.execute(
        f"""
        select
            superior_government_body_id as body_id,
            arg_max(superior_government_body_name, launch_month_key) as body_name,
            sum(total_amount_received_brl)::double as total_amount_brl
        from {SUMMARY_TABLE}
        where {where}
        group by 1
        order by 3 desc
        limit {int(limit)}
        """,
        params,
    ).df()


def kind_breakdown(con: duckdb.DuckDBPyConnection, filters: DashboardFilters) -> pd.DataFrame:
    where, params = summary_filter(filters)
    return con.execute(
        f"""
        select
            beneficiary_identifier_kind as identifier_kind,
            sum(total_amount_received_brl)::double as total_amount_brl,
            sum(total_record_count) as record_count
        from {SUMMARY_TABLE}
        where {where}
        group by 1
        order by 2 desc
        """,
        params,
    ).df()


def top_organizations(
    con: duckdb.DuckDBPyConnection, filters: DashboardFilters, limit: int = 15
) -> pd.DataFrame:
    """Largest CNPJ and foreign-code beneficiaries in the period.

    The beneficiary mart has no superior body, so this ranking follows the period
    and identifier-kind filters only.
    """
    kinds = tuple(kind for kind in filters.identifier_kinds if kind in ORGANIZATION_KINDS)
    if not kinds:
        return pd.DataFrame(columns=ORGANIZATION_COLUMNS)
    return con.execute(
        f"""
        select
            beneficiary_id,
            arg_max(beneficiary_name, launch_month_key) as beneficiary_name,
            any_value(beneficiary_identifier_kind) as identifier_kind,
            sum(total_amount_received_brl)::double as total_amount_brl,
            count(distinct launch_month_key) as month_count
        from {BENEFICIARY_TABLE}
        where launch_month_key between ? and ?
          and beneficiary_identifier_kind in ({placeholders(kinds)})
        group by 1
        order by 4 desc
        limit {int(limit)}
        """,
        [filters.start_month_key, filters.end_month_key, *kinds],
    ).df()


def source_freshness(manifest_path: Path = DEFAULT_SYNC_MANIFEST_PATH) -> SourceFreshness:
    """Read sync metadata; months registered while still open are partial."""
    entries = latest_entries_by_month(manifest_path).values()
    retrieved = [entry.retrieved_at for entry in entries if entry.retrieved_at]
    open_keys = frozenset(
        int(entry.reference_month.replace("-", ""))
        for entry in entries
        if entry.is_open_month and entry.reference_month
    )
    return SourceFreshness(max(retrieved) if retrieved else None, open_keys)
