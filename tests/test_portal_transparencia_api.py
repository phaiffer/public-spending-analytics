from __future__ import annotations

import json
from pathlib import Path

from gov_spending_analytics.ingestion.portal_transparencia_api import (
    build_despesas_documentos_request,
    build_url,
    default_despesas_documentos_output_dir,
    ingest_despesas_documentos,
    resolve_api_key,
)


class FakeDespesasDocumentosClient:
    base_url = "https://api.portaldatransparencia.gov.br"

    def fetch_despesas_documentos_page(self, request_params, pagina):
        if request_params.fase == 3 and request_params.unidade_gestora == "170013" and pagina == 1:
            return [{"codigo": "documento-1", "valor": 10}]
        return []


def test_resolve_api_key_reads_configured_environment_variable(monkeypatch) -> None:
    monkeypatch.setenv("CUSTOM_PORTAL_API_KEY", "secret-token")
    config = {
        "sources": {
            "portal_transparencia_api": {
                "api_key_env_var": "CUSTOM_PORTAL_API_KEY",
            }
        }
    }

    assert resolve_api_key(config) == "secret-token"


def test_request_builder_requires_additional_filter() -> None:
    try:
        build_despesas_documentos_request(
            data_emissao="02/01/2025",
            fase=3,
        )
    except ValueError as exc:
        assert "unidade-gestora" in str(exc)
    else:
        raise AssertionError("Expected missing filter validation to fail")


def test_request_builder_parses_required_date_and_phase() -> None:
    request = build_despesas_documentos_request(
        data_emissao="02/01/2025",
        fase=3,
        unidade_gestora="170013",
        pagina_inicial=2,
        max_paginas=5,
    )

    assert request.data_emissao.isoformat() == "2025-01-02"
    assert request.fase == 3
    assert request.to_api_params(pagina=2) == {
        "dataEmissao": "02/01/2025",
        "fase": 3,
        "pagina": 2,
        "unidadeGestora": "170013",
    }


def test_default_output_dir_uses_request_filter_folders() -> None:
    request = build_despesas_documentos_request(
        data_emissao="02/01/2025",
        fase=3,
        unidade_gestora="170013",
    )

    output_dir = default_despesas_documentos_output_dir(
        raw_data_path=Path("data/raw"),
        request_params=request,
    )

    assert str(output_dir).replace("\\", "/").endswith(
        "data/raw/portal_transparencia_api/despesas_documentos/"
        "data_emissao=2025-01-02/fase=3/unidade_gestora=170013"
    )


def test_build_url_uses_documented_query_parameters() -> None:
    url = build_url(
        base_url="https://api.portaldatransparencia.gov.br",
        endpoint="/api-de-dados/despesas/documentos",
        params={
            "dataEmissao": "02/01/2025",
            "fase": 3,
            "pagina": 1,
            "unidadeGestora": "170013",
        },
    )

    assert url == (
        "https://api.portaldatransparencia.gov.br/api-de-dados/despesas/documentos"
        "?dataEmissao=02%2F01%2F2025&fase=3&pagina=1&unidadeGestora=170013"
    )


def test_ingest_despesas_documentos_writes_raw_page_and_manifest(tmp_path) -> None:
    request = build_despesas_documentos_request(
        data_emissao="02/01/2025",
        fase=3,
        unidade_gestora="170013",
    )

    result = ingest_despesas_documentos(
        client=FakeDespesasDocumentosClient(),
        request_params=request,
        output_dir=tmp_path,
    )

    page_path = tmp_path / "page=0001.json"
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    assert result.page_count == 1
    assert result.total_records == 1
    assert page_path.exists()
    assert json.loads(page_path.read_text(encoding="utf-8")) == [
        {"codigo": "documento-1", "valor": 10}
    ]
    assert manifest["request_parameters"]["data_emissao"] == "2025-01-02"
    assert manifest["request_parameters"]["dataEmissao"] == "02/01/2025"
    assert manifest["request_parameters"]["fase"] == 3
    assert manifest["request_parameters"]["unidadeGestora"] == "170013"
    assert manifest["source_endpoint"] == "/api-de-dados/despesas/documentos"
