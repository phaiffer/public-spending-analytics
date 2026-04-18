"""Stage Portal da Transparencia recebimentos por favorecido CSV into Parquet."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd

from gov_spending_analytics.quality.basic_checks import (
    check_required_columns,
    check_source_traceability,
    raise_for_quality_failures,
)

SOURCE_SYSTEM = "portal_transparencia"
SOURCE_FAMILY = "recebimentos_recursos_por_favorecido"
CHUNK_SIZE = 100_000
EXPECTED_COLUMNS = [
    "Código Favorecido",
    "Nome Favorecido",
    "Sigla UF",
    "Nome Município",
    "Código Órgão Superior",
    "Nome Órgão Superior",
    "Código Órgão",
    "Nome Órgão",
    "Código Unidade Gestora",
    "Nome Unidade Gestora",
    "Ano e mês do lançamento",
    "Valor Recebido",
]
TRACEABILITY_COLUMNS = (
    "source_system",
    "source_family",
    "source_file_name",
    "source_file_path",
    "source_profile_name",
    "source_row_number",
)
REQUIRED_STAGED_COLUMNS = (
    "beneficiary_id",
    "beneficiary_name",
    "beneficiary_location_code",
    "beneficiary_municipality_name",
    "superior_government_body_id",
    "superior_government_body_name",
    "government_body_id",
    "government_body_name",
    "management_unit_id",
    "management_unit_name",
    "launch_month",
    "amount_received_brl",
)
SOURCE_TO_STAGED_COLUMNS = {
    "Código Favorecido": "beneficiary_id",
    "Nome Favorecido": "beneficiary_name",
    "Sigla UF": "beneficiary_location_code",
    "Nome Município": "beneficiary_municipality_name",
    "Código Órgão Superior": "superior_government_body_id",
    "Nome Órgão Superior": "superior_government_body_name",
    "Código Órgão": "government_body_id",
    "Nome Órgão": "government_body_name",
    "Código Unidade Gestora": "management_unit_id",
    "Nome Unidade Gestora": "management_unit_name",
    "Ano e mês do lançamento": "launch_month",
    "Valor Recebido": "amount_received_brl",
}


@dataclass(frozen=True)
class RecebimentosStagingResult:
    """Summary of a recebimentos raw-to-staging write."""

    output_path: Path
    row_count: int
    source_file: Path
    profile_path: Path
    source_family: str
    column_mapping: dict[str, str]


def stage_recebimentos_recursos_por_favorecido_csv(
    file_path: Path,
    profile_path: Path,
    output_path: Path | None = None,
) -> RecebimentosStagingResult:
    """Stage the profiled recebimentos por favorecido CSV into one Parquet file."""
    file_path = file_path.resolve()
    profile_path = profile_path.resolve()

    profile = load_profile(profile_path)
    validate_profile(profile=profile, file_path=file_path)

    metadata = profile["profile_metadata"]
    if output_path is None:
        output_path = default_output_path(file_path)
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    staged_chunks: list[pd.DataFrame] = []
    next_row_number = 1
    for raw_chunk in pd.read_csv(
        file_path,
        sep=metadata["delimiter"],
        encoding=metadata["encoding"],
        dtype=str,
        chunksize=CHUNK_SIZE,
        keep_default_na=False,
    ):
        raw_chunk = raw_chunk[EXPECTED_COLUMNS]
        staged_chunk = build_staged_chunk(
            raw_chunk=raw_chunk,
            file_path=file_path,
            profile_path=profile_path,
            first_row_number=next_row_number,
        )
        staged_chunks.append(staged_chunk)
        next_row_number += len(raw_chunk)

    if staged_chunks:
        staged = pd.concat(staged_chunks, ignore_index=True)
    else:
        staged = build_empty_staged_frame()

    validate_staged_data(staged)
    staged.to_parquet(output_path, index=False)

    return RecebimentosStagingResult(
        output_path=output_path,
        row_count=len(staged),
        source_file=file_path,
        profile_path=profile_path,
        source_family=SOURCE_FAMILY,
        column_mapping=SOURCE_TO_STAGED_COLUMNS,
    )


def load_profile(profile_path: Path) -> dict[str, Any]:
    """Load a local profiling artifact."""
    if not profile_path.exists():
        raise FileNotFoundError(f"Profile artifact not found: {profile_path}")
    return json.loads(profile_path.read_text(encoding="utf-8"))


def validate_profile(profile: dict[str, Any], file_path: Path) -> None:
    """Validate the profile against the confirmed recebimentos source facts."""
    metadata = profile.get("profile_metadata", {})
    if metadata.get("file_name") and metadata["file_name"] != file_path.name:
        raise ValueError(
            "Profile artifact file_name does not match the selected raw file: "
            f"{metadata['file_name']} != {file_path.name}"
        )

    expected_metadata = {
        "encoding": "latin-1",
        "delimiter": ";",
    }
    for field, expected_value in expected_metadata.items():
        actual_value = metadata.get(field)
        if actual_value != expected_value:
            raise ValueError(
                f"Expected profile metadata {field}={expected_value!r}, got {actual_value!r}"
            )

    if profile.get("row_count") != 300391:
        raise ValueError("Profile row_count does not match the confirmed source file")
    if profile.get("column_count") != len(EXPECTED_COLUMNS):
        raise ValueError("Profile column_count does not match the confirmed schema")
    if profile.get("columns") != EXPECTED_COLUMNS:
        raise ValueError("Profile columns do not match the confirmed observed schema")
    if profile.get("null_heavy_columns"):
        raise ValueError("Profile reported null-heavy columns for this source file")

    header_columns = read_csv_header(
        file_path=file_path,
        encoding=metadata["encoding"],
        delimiter=metadata["delimiter"],
    )
    if header_columns != EXPECTED_COLUMNS:
        raise ValueError(
            "Raw CSV header does not match the confirmed profile. Re-run profiling "
            "before staging this file."
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


def default_output_path(file_path: Path) -> Path:
    """Build the default staging path for the confirmed source file."""
    return (
        Path("data")
        / "staging"
        / SOURCE_SYSTEM
        / SOURCE_FAMILY
        / f"{file_path.stem}.parquet"
    )


def build_staged_chunk(
    raw_chunk: pd.DataFrame,
    file_path: Path,
    profile_path: Path,
    first_row_number: int,
) -> pd.DataFrame:
    """Build one staged dataframe chunk from source columns."""
    raw_chunk = raw_chunk.reset_index(drop=True)
    row_count = len(raw_chunk)
    staged = pd.DataFrame(
        {
            "source_system": SOURCE_SYSTEM,
            "source_family": SOURCE_FAMILY,
            "source_file_name": file_path.name,
            "source_file_path": str(file_path),
            "source_profile_name": profile_path.name,
            "source_row_number": range(first_row_number, first_row_number + row_count),
        }
    )
    staged["beneficiary_id"] = raw_chunk["Código Favorecido"].map(clean_text)
    staged["beneficiary_name"] = raw_chunk["Nome Favorecido"].map(clean_text)
    staged["beneficiary_location_code"] = raw_chunk["Sigla UF"].map(clean_text)
    staged["beneficiary_municipality_name"] = raw_chunk["Nome Município"].map(clean_text)
    staged["superior_government_body_id"] = raw_chunk["Código Órgão Superior"].map(clean_text)
    staged["superior_government_body_name"] = raw_chunk["Nome Órgão Superior"].map(clean_text)
    staged["government_body_id"] = raw_chunk["Código Órgão"].map(clean_text)
    staged["government_body_name"] = raw_chunk["Nome Órgão"].map(clean_text)
    staged["management_unit_id"] = raw_chunk["Código Unidade Gestora"].map(clean_text)
    staged["management_unit_name"] = raw_chunk["Nome Unidade Gestora"].map(clean_text)
    staged["launch_month"] = raw_chunk["Ano e mês do lançamento"].map(parse_launch_month)
    staged["amount_received_brl"] = raw_chunk["Valor Recebido"].map(parse_brazilian_decimal)
    return staged


def build_empty_staged_frame() -> pd.DataFrame:
    """Create an empty staged frame with the expected columns."""
    return pd.DataFrame(columns=list(TRACEABILITY_COLUMNS) + list(REQUIRED_STAGED_COLUMNS))


def validate_staged_data(staged: pd.DataFrame) -> None:
    """Run lightweight checks before writing staging output."""
    failures = []
    failures.extend(check_required_columns(staged, REQUIRED_STAGED_COLUMNS))
    failures.extend(check_source_traceability(staged, TRACEABILITY_COLUMNS))
    raise_for_quality_failures(failures)


def parse_launch_month(value: str | None) -> str | None:
    """Parse MM/YYYY source text into a YYYY-MM month key."""
    text = clean_text(value)
    if text is None:
        return None
    try:
        parsed = pd.to_datetime(text, format="%m/%Y", errors="raise")
    except ValueError as exc:
        raise ValueError(f"Could not parse launch month as MM/YYYY: {value!r}") from exc
    return parsed.strftime("%Y-%m")


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
