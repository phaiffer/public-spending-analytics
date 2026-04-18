from decimal import Decimal
from pathlib import Path

import pandas as pd

from gov_spending_analytics.staging.portal_transparencia_recebimentos import (
    EXPECTED_COLUMNS,
    build_staged_chunk,
    parse_brazilian_decimal,
    parse_launch_month,
)


def test_parse_launch_month_returns_month_key_not_daily_timestamp() -> None:
    assert parse_launch_month("01/2026") == "2026-01"


def test_parse_brazilian_decimal_preserves_monetary_precision() -> None:
    assert parse_brazilian_decimal("19.202,5200") == Decimal("19202.5200")


def test_build_staged_chunk_preserves_ids_and_exterior_location() -> None:
    raw_chunk = pd.DataFrame(
        [
            {
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
        ],
        columns=EXPECTED_COLUMNS,
    )
    raw_chunk.index = [100000]

    staged = build_staged_chunk(
        raw_chunk=raw_chunk,
        file_path=Path("data/raw/202601_RecebimentosRecursosPorFavorecido.csv"),
        profile_path=Path("profiling/202601_RecebimentosRecursosPorFavorecido_profile.json"),
        first_row_number=1,
    )

    assert staged.loc[0, "beneficiary_id"] == "06343056000125"
    assert staged.loc[0, "beneficiary_location_code"] == "EX"
    assert staged.loc[0, "beneficiary_municipality_name"] == "Exterior"
    assert staged.loc[0, "launch_month"] == "2026-01"
    assert staged.loc[0, "amount_received_brl"] == Decimal("19202.5200")
    assert staged.loc[0, "source_row_number"] == 1
