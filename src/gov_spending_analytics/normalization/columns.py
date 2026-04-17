"""Column-name normalization and canonical mapping utilities."""

from __future__ import annotations

import re
import unicodedata

CANONICAL_COLUMN_PATTERNS: dict[str, tuple[str, ...]] = {
    "spending_document_id": (
        r"\bnumero.*documento\b",
        r"\bdocumento\b",
        r"\bcodigo.*documento\b",
        r"\bnumero.*empenho\b",
        r"\bnumero.*liquidacao\b",
        r"\bnumero.*pagamento\b",
    ),
    "spending_date": (
        r"\bdata\b",
        r"\bdata.*emissao\b",
        r"\bdata.*documento\b",
        r"\bdata.*pagamento\b",
        r"\bdata.*liquidacao\b",
    ),
    "fiscal_year": (
        r"\bano\b",
        r"\bexercicio\b",
        r"\bano.*exercicio\b",
    ),
    "government_body_id": (
        r"\bcodigo.*orgao\b",
        r"\bcod.*orgao\b",
        r"\borgao.*codigo\b",
    ),
    "government_body_name": (
        r"\bnome.*orgao\b",
        r"\borgao\b",
        r"\borgao.*nome\b",
    ),
    "beneficiary_id": (
        r"\bcodigo.*favorecido\b",
        r"\bcpf\b",
        r"\bcnpj\b",
        r"\bcpf.*cnpj\b",
        r"\bfavorecido.*codigo\b",
    ),
    "beneficiary_name": (
        r"\bnome.*favorecido\b",
        r"\bfavorecido\b",
        r"\bfavorecido.*nome\b",
    ),
    "amount_brl": (
        r"\bvalor\b",
        r"\bvalor.*documento\b",
        r"\bvalor.*empenhado\b",
        r"\bvalor.*liquidado\b",
        r"\bvalor.*pago\b",
    ),
}

REQUIRED_STAGING_CANONICAL_COLUMNS = ("amount_brl",)


def normalize_column_name(column_name: str) -> str:
    """Convert source column labels to predictable snake_case names."""
    normalized = unicodedata.normalize("NFKD", column_name)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = ascii_text.strip().lower()
    ascii_text = re.sub(r"[^a-z0-9]+", "_", ascii_text)
    return ascii_text.strip("_")


def suggest_canonical_columns(columns: list[str]) -> dict[str, list[dict[str, str]]]:
    """Suggest canonical fields for observed source columns.

    Suggestions are intentionally non-authoritative. They help profiling surface
    likely mappings, but final mappings should be confirmed from source
    dictionaries and real record samples.
    """
    suggestions: dict[str, list[dict[str, str]]] = {}

    for source_column in columns:
        normalized = normalize_column_name(source_column)
        for canonical_name, patterns in CANONICAL_COLUMN_PATTERNS.items():
            if any(re.search(pattern, normalized) for pattern in patterns):
                suggestions.setdefault(canonical_name, []).append(
                    {
                        "source_column": source_column,
                        "normalized_column": normalized,
                    }
                )

    return suggestions


def resolve_unambiguous_canonical_mapping(
    canonical_suggestions: dict[str, list[dict[str, str]]],
) -> dict[str, str]:
    """Return canonical-to-source mappings only when profiling found one clear source column."""
    resolved: dict[str, str] = {}

    for canonical_name, candidates in canonical_suggestions.items():
        if len(candidates) == 1:
            resolved[canonical_name] = candidates[0]["source_column"]

    return resolved
