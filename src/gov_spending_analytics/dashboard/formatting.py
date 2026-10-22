"""Brazilian Portuguese number and month formatting for the dashboard."""

from __future__ import annotations

MONTH_ABBREVIATIONS = (
    "jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez",
)
SCALES = ((1e12, "tri"), (1e9, "bi"), (1e6, "mi"), (1e3, "mil"))


def format_number(value: float, decimals: int = 0) -> str:
    """Format with a dot thousands separator and a comma decimal mark."""
    text = f"{value:,.{decimals}f}"
    return text.replace(",", "\0").replace(".", ",").replace("\0", ".")


def format_brl(value: float) -> str:
    """Compact reais: R$ 1,23 bi, R$ 456,70 mi, R$ 980,10."""
    sign = "-" if value < 0 else ""
    magnitude = abs(value)
    for threshold, suffix in SCALES:
        if magnitude >= threshold:
            return f"{sign}R$ {format_number(magnitude / threshold, 2)} {suffix}"
    return f"{sign}R$ {format_number(magnitude, 2)}"


def amount_scale(max_value: float) -> tuple[float, str]:
    """Divisor and axis label for a chart whose largest value is max_value."""
    for threshold, suffix in SCALES:
        if abs(max_value) >= threshold:
            return threshold, f"R$ {suffix}"
    return 1.0, "R$"


def format_cnpj(value: str) -> str:
    """11222333000181 -> 11.222.333/0001-81; other identifiers are returned as is."""
    if len(value) != 14 or not value.isdigit():
        return value
    return f"{value[:2]}.{value[2:5]}.{value[5:8]}/{value[8:12]}-{value[12:]}"


def month_label(month_key: int) -> str:
    """202601 -> jan/2026."""
    return f"{MONTH_ABBREVIATIONS[month_key % 100 - 1]}/{month_key // 100}"
