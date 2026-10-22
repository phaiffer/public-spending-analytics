from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from gov_spending_analytics.staging.portal_transparencia_recebimentos import (
    EXPECTED_COLUMNS,
    build_staged_chunk,
    parse_brazilian_decimal,
    parse_launch_month,
    validate_staged_data,
)


def test_parse_launch_month_returns_month_key_not_daily_timestamp() -> None:
    assert parse_launch_month("01/2026") == "2026-01"


def test_parse_brazilian_decimal_preserves_monetary_precision() -> None:
    assert parse_brazilian_decimal("19.202,5200") == Decimal("19202.5200")


def raw_row(**overrides: str) -> dict[str, str]:
    row = {
        "Código Favorecido": "06343056000125",
        "Nome Favorecido": "FAVORECIDO TESTE",
        "Sigla UF": "EX",
        "Nome Município": "Exterior",
        "Código Órgão Superior": "36000",
        "Nome Órgão Superior": "Ministério da Saúde",
        "Código Órgão": "36000",
        "Nome Órgão": "Ministério da Saúde - Unidades com vínculo direto",
        "Código Unidade Gestora": "257001",
        "Nome Unidade Gestora": "DIRETORIA EXECUTIVA DO FUNDO NAC. DE SAUDE",
        "Ano e mês do lançamento": "01/2026",
        "Valor Recebido": "19202,5200",
    }
    return {**row, **overrides}


def stage_rows(*rows: dict[str, str]) -> pd.DataFrame:
    raw_chunk = pd.DataFrame(list(rows), columns=EXPECTED_COLUMNS)
    raw_chunk.index = range(100000, 100000 + len(rows))
    return build_staged_chunk(
        raw_chunk=raw_chunk,
        file_path=Path("data/raw/202601_RecebimentosRecursosPorFavorecido.csv"),
        profile_path=Path("profiling/202601_RecebimentosRecursosPorFavorecido_profile.json"),
        source_file_sha256="0" * 64,
        source_registered_row_count=len(rows),
        first_row_number=1,
    )


def test_build_staged_chunk_preserves_ids_and_exterior_location() -> None:
    staged = stage_rows(raw_row())

    assert staged.loc[0, "beneficiary_id"] == "06343056000125"
    assert staged.loc[0, "beneficiary_location_code"] == "EX"
    assert staged.loc[0, "beneficiary_municipality_name"] == "Exterior"
    assert staged.loc[0, "launch_month"] == "2026-01"
    assert staged.loc[0, "amount_received_brl"] == Decimal("19202.5200")
    assert staged.loc[0, "source_row_number"] == 1


def test_validate_allows_empty_body_name_behind_sentinel_code() -> None:
    staged = stage_rows(
        raw_row(),
        raw_row(**{
            "Código Órgão Superior": "-1",
            "Nome Órgão Superior": "Sem informação",
            "Código Órgão": "-3",
            "Nome Órgão": "",
        }),
    )

    assert staged["government_body_name"].isna().sum() == 1
    validate_staged_data(staged)


def test_validate_rejects_empty_body_name_behind_real_code() -> None:
    staged = stage_rows(raw_row(**{"Nome Órgão": ""}))

    with pytest.raises(ValueError, match="names_behind_sentinel_ids"):
        validate_staged_data(staged)
