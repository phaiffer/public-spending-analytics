"""DuckDB bootstrap utilities."""

from __future__ import annotations

from pathlib import Path


def bootstrap_duckdb(database_path: Path) -> Path:
    """Create a local DuckDB database file and return its resolved path."""
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError(
            "DuckDB is not installed. Install project dependencies with "
            'python -m pip install -e ".[dev]".'
        ) from exc

    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("select 1")

    return database_path.resolve()
