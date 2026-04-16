"""Helpers for Portal da Transparencia bulk files.

The MVP is intentionally local-first: users download official files manually and
place them under data/raw before ingestion code reads them.
"""

from __future__ import annotations

from pathlib import Path


def discover_raw_csv_files(raw_data_path: Path) -> list[Path]:
    """Return raw CSV files below the configured raw data directory."""
    if not raw_data_path.exists():
        return []

    return sorted(path for path in raw_data_path.rglob("*.csv") if path.is_file())


def describe_expected_source_layout() -> str:
    """Return a human-readable description of the recommended raw layout."""
    return "data/raw/portal_transparencia/despesas/YYYY/MM/"
