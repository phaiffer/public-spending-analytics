"""Write the synthetic Recebimentos de Recursos por Favorecido sample used by e2e tests.

The sample reproduces the official file format (latin-1, semicolon delimiter,
every field quoted, CRLF line endings, MM/YYYY months, Brazilian decimals) and
covers every beneficiary identifier kind, negative amounts, and one masked-CPF
name collision. Beneficiary names and identifiers are fictitious; government
body names and codes are public.

Run from the repository root after changing the rows, then update the SHA-256
and row count in tests/fixtures/source_manifest.toml:

    python tests/fixtures/make_recebimentos_sample.py
"""

from __future__ import annotations

import csv
from pathlib import Path

OUTPUT_PATH = (
    Path(__file__).parent / "recebimentos_sample" / "202601_RecebimentosRecursosPorFavorecido.csv"
)
HEADER = [
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
SAUDE = ("36000", "Ministério da Saúde", "36000", "Ministério da Saúde - Unidades com vínculo direto")
EDUCACAO = ("26000", "Ministério da Educação", "26000", "Ministério da Educação - Unidades com vínculo direto")
EXTERIORES = ("35000", "Ministério das Relações Exteriores", "35000", "Ministério das Relações Exteriores - Unidades com vínculo direto")
FAZENDA = ("25000", "Ministério da Fazenda", "25000", "Ministério da Fazenda - Unidades com vínculo direto")

# (beneficiary id, name, UF, municipality, body, unit id, unit name, amount)
ROWS = [
    ("11222333000181", "EMPRESA FICTICIA ALFA LTDA", "SP", "SÃO PAULO", SAUDE, "900001", "UNIDADE GESTORA TESTE UM", "1500,0000"),
    ("11222333000181", "EMPRESA FICTICIA ALFA LTDA", "SP", "SÃO PAULO", SAUDE, "900002", "UNIDADE GESTORA TESTE DOIS", "250,5000"),
    ("44555666000199", "EMPRESA FICTICIA BETA S.A.", "MG", "BELO HORIZONTE", EDUCACAO, "900003", "UNIDADE GESTORA TESTE TRÊS", "1234567,8900"),
    ("44555666000199", "EMPRESA FICTICIA BETA S.A.", "MG", "BELO HORIZONTE", EDUCACAO, "900003", "UNIDADE GESTORA TESTE TRÊS", "-1000,0000"),
    ("77888999000155", "COOPERATIVA FICTICIA GAMA", "RJ", "RIO DE JANEIRO", SAUDE, "900001", "UNIDADE GESTORA TESTE UM", "980,1000"),
    ("***.111.222-**", "PESSOA FICTICIA UM", "BA", "SALVADOR", EDUCACAO, "900003", "UNIDADE GESTORA TESTE TRÊS", "600,0000"),
    ("***.111.222-**", "PESSOA FICTICIA DOIS", "BA", "SALVADOR", EDUCACAO, "900003", "UNIDADE GESTORA TESTE TRÊS", "600,0000"),
    ("***.333.444-**", "PESSOA FICTICIA TRÊS", "PE", "RECIFE", SAUDE, "900001", "UNIDADE GESTORA TESTE UM", "-45,2500"),
    ("***.555.666-**", "PESSOA FICTICIA QUATRO", "RS", "PORTO ALEGRE", SAUDE, "900002", "UNIDADE GESTORA TESTE DOIS", "1200,0000"),
    ("EXFICTCOR", "FICTITIOUS CORPORATION ABROAD", "EX", "Exterior", EXTERIORES, "900004", "UNIDADE GESTORA TESTE QUATRO", "5000,0000"),
    ("EXFICTUNI", "FICTITIOUS UNIVERSITY ABROAD", "EX", "Exterior", EXTERIORES, "900004", "UNIDADE GESTORA TESTE QUATRO", "7500,0000"),
    ("-1", "SEM INFORMACAO", "DF", "BRASÍLIA", FAZENDA, "900005", "UNIDADE GESTORA TESTE CINCO", "300,0000"),
    ("-11", "Sigiloso", "DF", "BRASÍLIA", FAZENDA, "900005", "UNIDADE GESTORA TESTE CINCO", "800,0000"),
    ("-13", "FOLHA DE PAGAMENTO", "DF", "BRASÍLIA", FAZENDA, "900005", "UNIDADE GESTORA TESTE CINCO", "99999,9900"),
    ("999001", "UNIDADE GESTORA FICTICIA FAVORECIDA", "DF", "BRASÍLIA", FAZENDA, "900005", "UNIDADE GESTORA TESTE CINCO", "15000,0000"),
    ("RB0000001", "BENEFICIARIO FICTICIO RB", "GO", "GOIÂNIA", EDUCACAO, "900003", "UNIDADE GESTORA TESTE TRÊS", "123,4500"),
    ("170500", "UNIDADE FICTICIA DO TESOURO", "DF", "BRASÍLIA", FAZENDA, "900005", "UNIDADE GESTORA TESTE CINCO", "70000,0000"),
    ("PFFICT001", "PESQUISADORA FICTICIA NO EXTERIOR", "EX", "Exterior", EDUCACAO, "900003", "UNIDADE GESTORA TESTE TRÊS", "2500,0000"),
    ("900000000001", "SEM INFORMACAO", "DF", "BRASÍLIA", FAZENDA, "900005", "UNIDADE GESTORA TESTE CINCO", "10,0000"),
]


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="latin-1", newline="") as handle:
        writer = csv.writer(handle, delimiter=";", quoting=csv.QUOTE_ALL, lineterminator="\r\n")
        writer.writerow(HEADER)
        for beneficiary_id, name, uf, municipality, body, unit_id, unit_name, amount in ROWS:
            writer.writerow(
                [beneficiary_id, name, uf, municipality, *body, unit_id, unit_name, "01/2026", amount]
            )
    print(f"Wrote {len(ROWS)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
