from __future__ import annotations

from pathlib import Path

import pytest

from gov_spending_analytics.cli.main import build_parser, exclusive_lock


def test_exclusive_lock_refuses_a_second_holder(tmp_path: Path) -> None:
    lock_path = tmp_path / "sync" / "sync.lock"
    parser = build_parser()

    with exclusive_lock(lock_path, parser):
        with pytest.raises(SystemExit) as raised:
            with exclusive_lock(lock_path, parser):
                pass
        assert raised.value.code == 1

    with exclusive_lock(lock_path, parser):
        pass
