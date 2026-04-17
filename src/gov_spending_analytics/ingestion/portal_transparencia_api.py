"""Raw API ingestion for Portal da Transparencia despesas documents."""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "https://api.portaldatransparencia.gov.br"
DESPESAS_DOCUMENTOS_ENDPOINT = "/api-de-dados/despesas/documentos"
API_KEY_HEADER = "chave-api-dados"
DEFAULT_API_KEY_ENV_VAR = "PORTAL_TRANSPARENCIA_API_KEY"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
VALID_FASE_VALUES = {1, 2, 3}


@dataclass(frozen=True)
class DespesasDocumentosRequest:
    """Validated request parameters for despesas/documentos."""

    data_emissao: date
    fase: int
    unidade_gestora: str | None = None
    gestao: str | None = None
    pagina_inicial: int = 1
    max_paginas: int | None = None

    def to_api_params(self, pagina: int) -> dict[str, str | int]:
        """Return documented API query parameters for one page."""
        params: dict[str, str | int] = {
            "dataEmissao": self.data_emissao.strftime("%d/%m/%Y"),
            "fase": self.fase,
            "pagina": pagina,
        }
        if self.unidade_gestora:
            params["unidadeGestora"] = self.unidade_gestora
        if self.gestao:
            params["gestao"] = self.gestao
        return params

    def manifest_parameters(self) -> dict[str, Any]:
        """Return request parameters for the ingestion manifest."""
        return {
            "data_emissao": self.data_emissao.isoformat(),
            "dataEmissao": self.data_emissao.strftime("%d/%m/%Y"),
            "fase": self.fase,
            "unidadeGestora": self.unidade_gestora,
            "gestao": self.gestao,
            "pagina_inicial": self.pagina_inicial,
            "max_paginas": self.max_paginas,
        }


@dataclass(frozen=True)
class PortalTransparenciaApiClient:
    """Small client for the official Portal da Transparencia API."""

    api_key: str
    base_url: str = DEFAULT_BASE_URL
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    max_retries: int = DEFAULT_MAX_RETRIES

    def fetch_despesas_documentos_page(
        self,
        request_params: DespesasDocumentosRequest,
        pagina: int,
    ) -> list[dict[str, Any]]:
        """Fetch one documented despesas/documentos page."""
        params = request_params.to_api_params(pagina=pagina)
        url = build_url(self.base_url, DESPESAS_DOCUMENTOS_ENDPOINT, params)
        request = Request(
            url,
            headers={
                API_KEY_HEADER: self.api_key,
                "Accept": "application/json",
                "User-Agent": "gov-spending-analytics/0.1 local-ingestion",
            },
            method="GET",
        )

        payload = request_json_with_retries(
            request=request,
            timeout_seconds=self.timeout_seconds,
            max_retries=self.max_retries,
        )
        if not isinstance(payload, list):
            raise ValueError(
                "Expected the Portal da Transparencia despesas/documentos API to return "
                f"a JSON list, got {type(payload).__name__}"
            )
        return payload


@dataclass(frozen=True)
class ApiIngestionResult:
    """Summary of one raw API ingestion run."""

    output_dir: Path
    manifest_path: Path
    page_count: int
    total_records: int
    source_endpoint: str


def resolve_api_key(
    config: dict[str, Any],
    explicit_api_key: str | None = None,
) -> str:
    """Read the API key from CLI override, config-selected env var, or default env var."""
    if explicit_api_key:
        return explicit_api_key

    api_config = config.get("sources", {}).get("portal_transparencia_api", {})
    env_var = api_config.get("api_key_env_var", DEFAULT_API_KEY_ENV_VAR)
    api_key = os.environ.get(env_var)
    if not api_key:
        raise ValueError(
            "Portal da Transparencia API key not found. Set "
            f"{env_var} in the environment before running API ingestion."
        )
    return api_key


def build_despesas_documentos_request(
    data_emissao: str,
    fase: int,
    unidade_gestora: str | None = None,
    gestao: str | None = None,
    pagina_inicial: int = 1,
    max_paginas: int | None = None,
) -> DespesasDocumentosRequest:
    """Validate CLI parameters for the targeted despesas/documentos endpoint."""
    parsed_date = parse_brazilian_date(data_emissao)
    validate_fase(fase)
    validate_required_filter(unidade_gestora=unidade_gestora, gestao=gestao)
    validate_pagination(pagina_inicial=pagina_inicial, max_paginas=max_paginas)
    return DespesasDocumentosRequest(
        data_emissao=parsed_date,
        fase=fase,
        unidade_gestora=clean_optional_filter(unidade_gestora),
        gestao=clean_optional_filter(gestao),
        pagina_inicial=pagina_inicial,
        max_paginas=max_paginas,
    )


def ingest_despesas_documentos(
    client: PortalTransparenciaApiClient,
    request_params: DespesasDocumentosRequest,
    output_dir: Path,
) -> ApiIngestionResult:
    """Persist raw despesas/documentos API pages for one constrained request."""
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    extraction_started_at = datetime.now(UTC)
    raw_files: list[dict[str, Any]] = []
    page_count = 0
    total_records = 0
    empty_page_reached = False
    pagina = request_params.pagina_inicial

    while True:
        if request_params.max_paginas is not None and page_count >= request_params.max_paginas:
            break

        records = client.fetch_despesas_documentos_page(
            request_params=request_params,
            pagina=pagina,
        )
        if not records:
            empty_page_reached = True
            break

        page_path = output_dir / f"page={pagina:04d}.json"
        page_path.write_text(
            json.dumps(records, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        page_count += 1
        total_records += len(records)
        raw_files.append(
            {
                "pagina": pagina,
                "record_count": len(records),
                "path": str(page_path),
            }
        )
        pagina += 1

    manifest = {
        "source_system": "portal_transparencia_api",
        "source_endpoint": DESPESAS_DOCUMENTOS_ENDPOINT,
        "base_url": client.base_url,
        "request_parameters": request_params.manifest_parameters(),
        "documented_api_parameters_used": [
            "dataEmissao",
            "fase",
            "pagina",
            "unidadeGestora",
            "gestao",
        ],
        "extraction_started_at_utc": extraction_started_at.isoformat(),
        "extraction_finished_at_utc": datetime.now(UTC).isoformat(),
        "pages_fetched": page_count,
        "records_fetched": total_records,
        "empty_page_reached": empty_page_reached,
        "raw_response_files": raw_files,
        "notes": [
            "Raw API responses are persisted without normalization.",
            (
                "The despesas/documentos endpoint is constrained to one dataEmissao "
                "and at least one additional filter."
            ),
            "Pagination stops at the first empty JSON list or the optional max_paginas limit.",
        ],
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    return ApiIngestionResult(
        output_dir=output_dir,
        manifest_path=manifest_path,
        page_count=page_count,
        total_records=total_records,
        source_endpoint=DESPESAS_DOCUMENTOS_ENDPOINT,
    )


def default_despesas_documentos_output_dir(
    raw_data_path: Path,
    request_params: DespesasDocumentosRequest,
) -> Path:
    """Build the local raw API output folder."""
    output_dir = (
        raw_data_path
        / "portal_transparencia_api"
        / "despesas_documentos"
        / f"data_emissao={request_params.data_emissao.isoformat()}"
        / f"fase={request_params.fase}"
    )
    if request_params.unidade_gestora:
        unidade_gestora = safe_path_segment(request_params.unidade_gestora)
        output_dir = output_dir / f"unidade_gestora={unidade_gestora}"
    if request_params.gestao:
        output_dir = output_dir / f"gestao={safe_path_segment(request_params.gestao)}"
    return output_dir


def build_api_client_from_config(
    config: dict[str, Any],
    api_key: str,
) -> PortalTransparenciaApiClient:
    """Create the Portal API client from project config."""
    api_config = config.get("sources", {}).get("portal_transparencia_api", {})
    return PortalTransparenciaApiClient(
        api_key=api_key,
        base_url=api_config.get("base_url", DEFAULT_BASE_URL),
        timeout_seconds=int(api_config.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)),
        max_retries=int(api_config.get("max_retries", DEFAULT_MAX_RETRIES)),
    )


def build_url(base_url: str, endpoint: str, params: dict[str, Any]) -> str:
    """Build an API URL with encoded query parameters."""
    url = urljoin(base_url.rstrip("/") + "/", endpoint.lstrip("/"))
    return f"{url}?{urlencode(params)}"


def request_json_with_retries(
    request: Request,
    timeout_seconds: int,
    max_retries: int,
) -> Any:
    """Execute a JSON GET request with small local-first retry handling."""
    for attempt in range(1, max_retries + 1):
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                body = response.read().decode("utf-8")
                return json.loads(body)
        except HTTPError as exc:
            if exc.code in {401, 403}:
                raise ValueError(
                    "Portal da Transparencia API rejected the request. Check the API key."
                ) from exc
            if exc.code not in RETRYABLE_STATUS_CODES or attempt == max_retries:
                raise RuntimeError(
                    f"Portal da Transparencia API request failed with HTTP {exc.code}"
                ) from exc
            sleep_before_retry(exc, attempt)
        except URLError as exc:
            if attempt == max_retries:
                raise RuntimeError(
                    "Portal da Transparencia API request failed after retries"
                ) from exc
            sleep_before_retry(None, attempt)
        except json.JSONDecodeError as exc:
            raise ValueError("Portal da Transparencia API returned invalid JSON") from exc

    raise RuntimeError("Portal da Transparencia API request failed after retries")


def sleep_before_retry(error: HTTPError | None, attempt: int) -> None:
    """Sleep briefly before retrying a local API request."""
    retry_after = error.headers.get("Retry-After") if error else None
    if retry_after:
        try:
            time.sleep(min(int(retry_after), 30))
            return
        except ValueError:
            pass
    time.sleep(min(2 ** (attempt - 1), 10))


def parse_brazilian_date(value: str) -> date:
    """Parse a required DD/MM/YYYY date."""
    try:
        return datetime.strptime(value, "%d/%m/%Y").date()
    except ValueError as exc:
        raise ValueError(f"Expected dataEmissao in DD/MM/YYYY format, got {value!r}") from exc


def validate_fase(fase: int) -> None:
    """Validate documented despesas/documentos phase values."""
    if fase not in VALID_FASE_VALUES:
        raise ValueError("Expected fase to be one of 1, 2, or 3")


def validate_required_filter(unidade_gestora: str | None, gestao: str | None) -> None:
    """Require at least one additional endpoint filter."""
    if not clean_optional_filter(unidade_gestora) and not clean_optional_filter(gestao):
        raise ValueError("Provide at least one of --unidade-gestora or --gestao")


def validate_pagination(pagina_inicial: int, max_paginas: int | None) -> None:
    """Validate pagination controls."""
    if pagina_inicial < 1:
        raise ValueError("Expected pagina_inicial to be greater than or equal to 1")
    if max_paginas is not None and max_paginas < 1:
        raise ValueError("Expected max_paginas to be greater than or equal to 1")


def clean_optional_filter(value: str | None) -> str | None:
    """Trim optional API filters."""
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def safe_path_segment(value: str) -> str:
    """Make a request filter safe for a Windows folder segment."""
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", value.strip())
