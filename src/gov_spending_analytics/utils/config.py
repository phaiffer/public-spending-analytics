"""Project configuration loading."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any


def load_project_config(config_path: Path) -> dict[str, Any]:
    """Load project configuration from TOML."""
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with config_path.open("rb") as config_file:
        return tomllib.load(config_file)
