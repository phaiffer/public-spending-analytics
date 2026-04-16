"""Profile manually downloaded Portal da Transparencia CSV files."""

from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from gov_spending_analytics.normalization.columns import (
    normalize_column_name,
    suggest_canonical_columns,
)

NULL_TOKENS = {"", "null", "none", "nan", "na", "n/a", "-"}
ENCODING_CANDIDATES = ("utf-8-sig", "utf-8", "latin-1")
DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%Y%m%d", "%d-%m-%Y")
DATETIME_FORMATS = ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S")


def select_raw_csv_file(raw_data_path: Path, file_path: Path | None, pattern: str | None) -> Path:
    """Select one raw CSV file from either an explicit path or raw-data discovery."""
    if file_path:
        selected_path = file_path
        if not selected_path.is_absolute():
            selected_path = Path.cwd() / selected_path
        if not selected_path.exists():
            raise FileNotFoundError(f"Raw file not found: {selected_path}")
        if selected_path.suffix.lower() != ".csv":
            raise ValueError(f"Expected a CSV file, got: {selected_path}")
        return selected_path

    candidates = sorted(path for path in raw_data_path.rglob("*.csv") if path.is_file())
    if pattern:
        candidates = [path for path in candidates if pattern.lower() in str(path).lower()]

    if not candidates:
        raise FileNotFoundError(
            f"No raw CSV files found under {raw_data_path}. "
            "Place one manually downloaded Portal da Transparencia CSV file there first."
        )

    if len(candidates) > 1:
        candidate_list = "\n".join(f"- {path}" for path in candidates[:20])
        raise ValueError(
            "More than one raw CSV file matched. Pass --file or --pattern to select one.\n"
            f"{candidate_list}"
        )

    return candidates[0]


def profile_raw_csv_file(
    file_path: Path,
    output_path: Path,
    sample_size: int = 20,
    inference_rows: int = 5_000,
    null_threshold: float = 0.8,
) -> Path:
    """Write a JSON profile summary for a raw CSV file."""
    if sample_size < 0:
        raise ValueError("sample_size must be greater than or equal to zero")
    if inference_rows < 1:
        raise ValueError("inference_rows must be greater than zero")

    encoding = detect_encoding(file_path)
    delimiter = detect_delimiter(file_path, encoding)
    profile = build_profile(
        file_path=file_path,
        encoding=encoding,
        delimiter=delimiter,
        sample_size=sample_size,
        inference_rows=inference_rows,
        null_threshold=null_threshold,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(profile, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path.resolve()


def build_profile(
    file_path: Path,
    encoding: str,
    delimiter: str,
    sample_size: int,
    inference_rows: int,
    null_threshold: float,
) -> dict[str, Any]:
    """Build a profile dictionary for a CSV file."""
    row_count = 0
    sample_records: list[dict[str, str]] = []
    type_counters: dict[str, Counter[str]] = {}
    null_counts: Counter[str] = Counter()
    non_null_counts: Counter[str] = Counter()

    with file_path.open("r", encoding=encoding, newline="") as csv_file:
        reader = csv.DictReader(csv_file, delimiter=delimiter)
        if reader.fieldnames is None:
            raise ValueError(f"Could not read a header row from {file_path}")

        columns = list(reader.fieldnames)
        normalized_columns = {column: normalize_column_name(column) for column in columns}
        type_counters = {column: Counter() for column in columns}

        for row in reader:
            row_count += 1

            if len(sample_records) < sample_size:
                sample_records.append({column: row.get(column, "") for column in columns})

            if row_count <= inference_rows:
                for column in columns:
                    value = row.get(column, "")
                    if is_null(value):
                        null_counts[column] += 1
                        type_counters[column]["null"] += 1
                        continue

                    non_null_counts[column] += 1
                    type_counters[column][infer_value_type(value)] += 1

    inferred_types = {
        column: infer_column_type(type_counters[column]) for column in type_counters
    }
    profiled_row_count = min(row_count, inference_rows)
    column_profiles = []

    for column in columns:
        null_count = null_counts[column]
        null_ratio = null_count / profiled_row_count if profiled_row_count else 0
        column_profiles.append(
            {
                "source_column": column,
                "normalized_column": normalized_columns[column],
                "inferred_type": inferred_types[column],
                "null_count_in_profiled_rows": null_count,
                "non_null_count_in_profiled_rows": non_null_counts[column],
                "null_ratio_in_profiled_rows": round(null_ratio, 6),
                "is_null_heavy": null_ratio >= null_threshold,
                "type_counts": dict(type_counters[column]),
            }
        )

    return {
        "profile_metadata": {
            "profiled_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_file": str(file_path),
            "file_name": file_path.name,
            "file_size_bytes": file_path.stat().st_size,
            "encoding": encoding,
            "delimiter": delimiter,
            "sample_size": sample_size,
            "inference_rows": inference_rows,
            "null_threshold": null_threshold,
        },
        "row_count": row_count,
        "column_count": len(columns),
        "columns": columns,
        "column_profiles": column_profiles,
        "null_heavy_columns": [
            item["source_column"] for item in column_profiles if item["is_null_heavy"]
        ],
        "canonical_column_suggestions": suggest_canonical_columns(columns),
        "sample_records": sample_records,
        "profiling_notes": [
            "Type inference is sample-based and should be reviewed before modeling.",
            "Canonical column suggestions are heuristic and not final mappings.",
            "The final fact grain should not be decided from one file alone.",
        ],
    }


def detect_encoding(file_path: Path) -> str:
    """Return the first candidate encoding that can decode the file sample."""
    sample = file_path.read_bytes()[:100_000]
    for encoding in ENCODING_CANDIDATES:
        try:
            sample.decode(encoding)
        except UnicodeDecodeError:
            continue
        return encoding
    return "latin-1"


def detect_delimiter(file_path: Path, encoding: str) -> str:
    """Detect the CSV delimiter, defaulting to semicolon for Brazilian public CSVs."""
    sample = file_path.read_text(encoding=encoding, errors="replace")[:20_000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
    except csv.Error:
        return ";"
    return dialect.delimiter


def is_null(value: str | None) -> bool:
    """Return whether a source value should be treated as null for profiling."""
    if value is None:
        return True
    return value.strip().lower() in NULL_TOKENS


def infer_value_type(value: str) -> str:
    """Infer a basic scalar type for one value."""
    text = value.strip()

    if parse_int(text):
        return "integer"
    if parse_decimal(text):
        return "decimal"
    if parse_datetime(text):
        return "datetime"
    if parse_date(text):
        return "date"
    return "string"


def parse_int(value: str) -> bool:
    """Return whether a value can be parsed as an integer."""
    try:
        int(value)
    except ValueError:
        return False
    return True


def parse_decimal(value: str) -> bool:
    """Return whether a value can be parsed as a decimal, including Brazilian format."""
    normalized = value.replace(".", "").replace(",", ".")
    try:
        Decimal(normalized)
    except InvalidOperation:
        return False
    return True


def parse_date(value: str) -> bool:
    """Return whether a value matches one of the expected date formats."""
    for date_format in DATE_FORMATS:
        try:
            datetime.strptime(value, date_format)
        except ValueError:
            continue
        return True
    return False


def parse_datetime(value: str) -> bool:
    """Return whether a value matches one of the expected datetime formats."""
    for datetime_format in DATETIME_FORMATS:
        try:
            datetime.strptime(value, datetime_format)
        except ValueError:
            continue
        return True
    return False


def infer_column_type(type_counts: Counter[str]) -> str:
    """Infer the dominant non-null type for a column."""
    non_null_counts = Counter(
        {value_type: count for value_type, count in type_counts.items() if value_type != "null"}
    )
    if not non_null_counts:
        return "null"

    if len(non_null_counts) == 1:
        return next(iter(non_null_counts))

    numeric_types = {"integer", "decimal"}
    if set(non_null_counts).issubset(numeric_types):
        return "decimal" if "decimal" in non_null_counts else "integer"

    temporal_types = {"date", "datetime"}
    if set(non_null_counts).issubset(temporal_types):
        return "datetime" if "datetime" in non_null_counts else "date"

    return "string"
