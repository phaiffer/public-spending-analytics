"""Run the dbt project from Python entry points."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

DEFAULT_DBT_PROJECT_DIR = Path("dbt")


def find_dbt_executable() -> str | None:
    """Prefer the dbt installed next to the running interpreter, then PATH."""
    # which() adds .exe on Windows, where the venv keeps it under Scripts\.
    return shutil.which("dbt", path=str(Path(sys.executable).parent)) or shutil.which("dbt")


def run_dbt_build(project_dir: Path = DEFAULT_DBT_PROJECT_DIR) -> int:
    """Run `dbt build` with the project's local profiles.yml and return its exit code."""
    dbt = find_dbt_executable()
    if dbt is None:
        raise RuntimeError("dbt CLI not found; install the dev extra: uv sync --extra dev")
    completed = subprocess.run(
        [dbt, "build", "--profiles-dir", "."],
        cwd=project_dir,
        check=False,
    )
    return completed.returncode
