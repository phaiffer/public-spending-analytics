"""Small quality-check helpers for early ingestion work."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import pandas as pd


def find_missing_required_columns(
    available_columns: Iterable[str],
    required_columns: Iterable[str],
) -> list[str]:
    """Return required columns that are not present in a dataset."""
    available = set(available_columns)
    return [column for column in required_columns if column not in available]


@dataclass(frozen=True)
class QualityCheckFailure:
    """One lightweight data quality failure."""

    check_name: str
    message: str


def check_required_columns(
    frame: pd.DataFrame,
    required_columns: Iterable[str],
) -> list[QualityCheckFailure]:
    """Check that required columns exist and are populated."""
    failures: list[QualityCheckFailure] = []
    missing_columns = find_missing_required_columns(frame.columns, required_columns)
    if missing_columns:
        failures.append(
            QualityCheckFailure(
                check_name="required_columns_present",
                message=f"Missing required staged columns: {missing_columns}",
            )
        )

    for column in required_columns:
        if column not in frame.columns:
            continue
        null_count = int(frame[column].isna().sum())
        if null_count:
            failures.append(
                QualityCheckFailure(
                    check_name="required_columns_not_null",
                    message=f"Required staged column {column!r} has {null_count} null values",
                )
            )

    return failures


def check_non_negative_amount(
    frame: pd.DataFrame,
    amount_column: str = "amount_brl",
) -> list[QualityCheckFailure]:
    """Check that observed amount values are non-negative."""
    if amount_column not in frame.columns:
        return [
            QualityCheckFailure(
                check_name="amount_column_present",
                message=f"Amount column {amount_column!r} is missing",
            )
        ]

    negative_count = int((frame[amount_column].dropna() < 0).sum())
    if negative_count:
        return [
            QualityCheckFailure(
                check_name="amount_non_negative",
                message=f"Amount column {amount_column!r} has {negative_count} negative values",
            )
        ]
    return []


def check_source_traceability(
    frame: pd.DataFrame,
    traceability_columns: Iterable[str],
) -> list[QualityCheckFailure]:
    """Check that staged rows retain source traceability fields."""
    failures = check_required_columns(frame, traceability_columns)

    if "source_row_number" in frame.columns:
        invalid_row_numbers = int((frame["source_row_number"].dropna() < 1).sum())
        if invalid_row_numbers:
            failures.append(
                QualityCheckFailure(
                    check_name="source_row_number_positive",
                    message=f"source_row_number has {invalid_row_numbers} values below 1",
                )
            )

    return failures


def raise_for_quality_failures(failures: Iterable[QualityCheckFailure]) -> None:
    """Raise a compact ValueError for any quality-check failures."""
    failure_list = list(failures)
    if not failure_list:
        return

    messages = [f"{failure.check_name}: {failure.message}" for failure in failure_list]
    raise ValueError("Staged data failed quality checks: " + "; ".join(messages))
