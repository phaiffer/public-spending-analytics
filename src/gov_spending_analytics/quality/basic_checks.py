"""Small quality-check helpers for early ingestion work."""

from __future__ import annotations

from collections.abc import Iterable


def find_missing_required_columns(
    available_columns: Iterable[str],
    required_columns: Iterable[str],
) -> list[str]:
    """Return required columns that are not present in a dataset."""
    available = set(available_columns)
    return [column for column in required_columns if column not in available]
