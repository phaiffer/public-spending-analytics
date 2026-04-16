"""Column-name normalization utilities."""

from __future__ import annotations

import re
import unicodedata


def normalize_column_name(column_name: str) -> str:
    """Convert source column labels to predictable snake_case names."""
    normalized = unicodedata.normalize("NFKD", column_name)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = ascii_text.strip().lower()
    ascii_text = re.sub(r"[^a-z0-9]+", "_", ascii_text)
    return ascii_text.strip("_")
