"""Stage one profiled Portal da Transparencia despesas CSV into Parquet."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd

from gov_spending_analytics.normalization.columns import (
    REQUIRED_STAGING_CANONICAL_COLUMNS,
    normalize_column_name,
    resolve_unambiguous_canonical_mapping,
)
from gov_spending_analytics.quality.basic_checks import (
    check_non_negative_amount,
    check_required_columns,
    check_source_traceability,
    raise_for_quality_failures,
)

SOURCE_SYSTEM = "portal_transparencia"
SOURCE_FAMILY = "despesas"
CHUNK_SIZE = 100_000
TRACEABILITY_COLUMNS = (
    "source_system",
    "source_family",
    "source_file_name",
    "source_file_path",
    "source_profile_name",
    "source_row_number",
    "spending_stage",
)


@dataclass(frozen=True)
class StagingResult:
    """Summary of a raw-to-staging write."""

    output_path: Path
    row_count: int
    source_file: Path
    profile_path: Path
    source_family: str
    spending_stage: str
    canonical_mapping: dict[str, str]


def stage_profiled_despesas_csv(
    file_path: Path,
    profile_path: Path,
    output_path: Path | None = None,
) -> StagingResult:
    """Stage one profiled despesas CSV into one local Parquet file."""
    file_path = file_path.resolve()
    profile_path = profile_path.resolve()

    profile = load_profile(profile_path)
    validate_profile_for_file(profile, file_path)

    delimiter = profile["profile_metadata"]["delimiter"]
    encoding = profile["profile_metadata"]["encoding"]
    source_columns = list(profile["columns"])
    normalized_columns = normalize_unique_columns(source_columns)
    canonical_mapping = resolve_unambiguous_canonical_mapping(
        profile.get("canonical_column_suggestions", {})
    )
    validate_canonical_mapping_sources(canonical_mapping, source_columns)
    validate_required_canonical_mapping(canonical_mapping)

    spending_stage = infer_spending_stage(file_path.name)
    validate_source_family(file_path)

    if output_path is None:
        output_path = default_output_path(file_path, spending_stage)
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    staged_chunks: list[pd.DataFrame] = []
    next_row_number = 1
    for raw_chunk in pd.read_csv(
        file_path,
        sep=delimiter,
        encoding=encoding,
        dtype=str,
        chunksize=CHUNK_SIZE,
        keep_default_na=False,
    ):
        raw_chunk = raw_chunk[source_columns].rename(columns=normalized_columns)
        row_count = len(raw_chunk)
        staged_chunk = build_staged_chunk(
            raw_chunk=raw_chunk,
            normalized_columns=normalized_columns,
            canonical_mapping=canonical_mapping,
            file_path=file_path,
            profile_path=profile_path,
            spending_stage=spending_stage,
            first_row_number=next_row_number,
        )
        staged_chunks.append(staged_chunk)
        next_row_number += row_count

    if staged_chunks:
        staged = pd.concat(staged_chunks, ignore_index=True)
    else:
        staged = build_empty_staged_frame(
            normalized_columns=normalized_columns,
            canonical_mapping=canonical_mapping,
        )

    validate_staged_data(staged)
    staged.to_parquet(output_path, index=False)

    return StagingResult(
        output_path=output_path,
        row_count=len(staged),
        source_file=file_path,
        profile_path=profile_path,
        source_family=SOURCE_FAMILY,
        spending_stage=spending_stage,
        canonical_mapping=canonical_mapping,
    )


def load_profile(profile_path: Path) -> dict[str, Any]:
    """Load a local profiling artifact."""
    if not profile_path.exists():
        raise FileNotFoundError(f"Profile artifact not found: {profile_path}")
    return json.loads(profile_path.read_text(encoding="utf-8"))


def validate_profile_for_file(profile: dict[str, Any], file_path: Path) -> None:
    """Ensure the profile artifact describes the selected raw file header."""
    metadata = profile.get("profile_metadata", {})
    if metadata.get("file_name") and metadata["file_name"] != file_path.name:
        raise ValueError(
            "Profile artifact file_name does not match the selected raw file: "
            f"{metadata['file_name']} != {file_path.name}"
        )

    required_metadata = ("encoding", "delimiter")
    missing_metadata = [field for field in required_metadata if field not in metadata]
    if missing_metadata:
        raise ValueError(f"Profile artifact is missing metadata fields: {missing_metadata}")

    profile_columns = profile.get("columns")
    if not profile_columns:
        raise ValueError("Profile artifact does not contain observed columns")

    header_columns = read_csv_header(
        file_path=file_path,
        encoding=metadata["encoding"],
        delimiter=metadata["delimiter"],
    )
    if header_columns != profile_columns:
        raise ValueError(
            "Raw CSV header does not match the profiling artifact. "
            "Re-run profiling before staging this file."
        )


def read_csv_header(file_path: Path, encoding: str, delimiter: str) -> list[str]:
    """Read only the CSV header using pandas' parser."""
    header = pd.read_csv(
        file_path,
        sep=delimiter,
        encoding=encoding,
        dtype=str,
        nrows=0,
    )
    return list(header.columns)


def normalize_unique_columns(source_columns: list[str]) -> dict[str, str]:
    """Normalize observed columns and reject collisions."""
    normalized_columns = {
        source_column: f"source__{normalize_column_name(source_column)}"
        for source_column in source_columns
    }
    duplicate_names = {
        normalized_name
        for normalized_name in normalized_columns.values()
        if list(normalized_columns.values()).count(normalized_name) > 1
    }
    if duplicate_names:
        raise ValueError(
            "Normalized source column names are not unique: "
            f"{sorted(duplicate_names)}. Resolve before staging."
        )
    return normalized_columns


def validate_required_canonical_mapping(canonical_mapping: dict[str, str]) -> None:
    """Require the minimal fields needed for the first spending staging path."""
    missing = [
        canonical_name
        for canonical_name in REQUIRED_STAGING_CANONICAL_COLUMNS
        if canonical_name not in canonical_mapping
    ]
    if missing:
        raise ValueError(
            "The profiling artifact did not produce unambiguous mappings for required "
            f"staging fields: {missing}. Review the profile before staging."
        )


def validate_canonical_mapping_sources(
    canonical_mapping: dict[str, str],
    source_columns: list[str],
) -> None:
    """Ensure profile mappings only point at observed source columns."""
    observed_columns = set(source_columns)
    invalid_mappings = {
        canonical_name: source_column
        for canonical_name, source_column in canonical_mapping.items()
        if source_column not in observed_columns
    }
    if invalid_mappings:
        raise ValueError(
            "The profiling artifact contains canonical mappings to columns that were "
            f"not observed in the profiled header: {invalid_mappings}"
        )


def validate_staged_data(staged: pd.DataFrame) -> None:
    """Run lightweight source-evidence checks before writing staging output."""
    failures = []
    failures.extend(check_required_columns(staged, REQUIRED_STAGING_CANONICAL_COLUMNS))
    failures.extend(check_non_negative_amount(staged, "amount_brl"))
    failures.extend(check_source_traceability(staged, TRACEABILITY_COLUMNS))
    raise_for_quality_failures(failures)


def infer_spending_stage(file_name: str) -> str:
    """Infer the spending stage from the official despesas file name family."""
    normalized_name = normalize_column_name(Path(file_name).stem)

    if re.search(r"(^|_)despesas_item_empenho($|_)", normalized_name):
        return "commitment"
    if re.search(r"(^|_)despesas_empenho($|_)", normalized_name):
        return "commitment"
    if re.search(r"(^|_)despesas_liquidacao($|_)", normalized_name):
        return "liquidation"
    if re.search(r"(^|_)despesas_pagamento($|_)", normalized_name):
        return "payment"

    raise ValueError(
        "Could not infer spending stage from file name. Expected a Portal da "
        "Transparencia despesas file such as *_Despesas_Empenho.csv, "
        "*_Despesas_Liquidacao.csv, or *_Despesas_Pagamento.csv."
    )


def validate_source_family(file_path: Path) -> None:
    """Keep the first staging implementation limited to Portal despesas files."""
    normalized_path_parts = {normalize_column_name(part) for part in file_path.parts}
    normalized_name = normalize_column_name(file_path.name)
    looks_like_despesas = SOURCE_FAMILY in normalized_path_parts or "_despesas_" in normalized_name

    if not looks_like_despesas:
        raise ValueError(
            "This staging command is limited to the Portal da Transparencia despesas "
            "source family. Put the official file under a despesas path or keep the "
            "original *_Despesas_*.csv name."
        )


def default_output_path(file_path: Path, spending_stage: str) -> Path:
    """Build a practical local Parquet output path for one source file."""
    return (
        Path("data")
        / "staging"
        / SOURCE_SYSTEM
        / SOURCE_FAMILY
        / spending_stage
        / f"{file_path.stem}.parquet"
    )


def build_staged_chunk(
    raw_chunk: pd.DataFrame,
    normalized_columns: dict[str, str],
    canonical_mapping: dict[str, str],
    file_path: Path,
    profile_path: Path,
    spending_stage: str,
    first_row_number: int,
) -> pd.DataFrame:
    """Build one staged dataframe chunk from normalized source columns."""
    staged = pd.DataFrame(
        {
            "source_system": SOURCE_SYSTEM,
            "source_family": SOURCE_FAMILY,
            "source_file_name": file_path.name,
            "source_file_path": str(file_path),
            "source_profile_name": profile_path.name,
            "source_row_number": range(first_row_number, first_row_number + len(raw_chunk)),
            "spending_stage": spending_stage,
        }
    )

    for canonical_name, source_column in canonical_mapping.items():
        source_column_name = normalized_columns[source_column]
        if canonical_name == "amount_brl":
            staged[canonical_name] = raw_chunk[source_column_name].map(parse_brazilian_decimal)
        elif canonical_name == "spending_date":
            staged[canonical_name] = pd.to_datetime(
                raw_chunk[source_column_name].replace("", pd.NA),
                dayfirst=True,
                errors="coerce",
            ).dt.date
        elif canonical_name == "fiscal_year":
            staged[canonical_name] = pd.to_numeric(
                raw_chunk[source_column_name].replace("", pd.NA),
                errors="coerce",
            ).astype("Int64")
        else:
            staged[canonical_name] = raw_chunk[source_column_name].map(clean_text)

    return pd.concat([staged, raw_chunk.reset_index(drop=True)], axis=1)


def build_empty_staged_frame(
    normalized_columns: dict[str, str],
    canonical_mapping: dict[str, str],
) -> pd.DataFrame:
    """Create an empty staged frame with the expected columns."""
    columns = (
        list(TRACEABILITY_COLUMNS)
        + list(canonical_mapping)
        + list(normalized_columns.values())
    )
    return pd.DataFrame(columns=columns)


def parse_brazilian_decimal(value: str | None) -> Decimal | None:
    """Parse Brazilian decimal text such as 1.234,56 into Decimal."""
    text = clean_text(value)
    if text is None:
        return None
    normalized = text.replace(".", "").replace(",", ".")
    try:
        return Decimal(normalized)
    except InvalidOperation as exc:
        raise ValueError(f"Could not parse amount as decimal: {value!r}") from exc


def clean_text(value: str | None) -> str | None:
    """Trim source strings and convert blanks to null."""
    if value is None:
        return None
    text = value.strip()
    return text or None
