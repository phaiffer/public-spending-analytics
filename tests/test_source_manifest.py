from pathlib import Path

import pytest

from gov_spending_analytics.ingestion.source_manifest import (
    file_sha256,
    load_source_manifest,
    resolve_source_file_entry,
)

FAMILY = "recebimentos_recursos_por_favorecido"


def write_manifest(path: Path, file_name: str, sha256: str, row_count: int = 1) -> Path:
    path.write_text(
        "[[files]]\n"
        f'file_name = "{file_name}"\n'
        f'source_family = "{FAMILY}"\n'
        f'sha256 = "{sha256}"\n'
        f"row_count = {row_count}\n"
        "column_count = 12\n"
        'retrieved_at = "2026-10-21"\n',
        encoding="utf-8",
    )
    return path


def test_resolve_source_file_entry_matches_name_and_hash(tmp_path: Path) -> None:
    raw_file = tmp_path / "202601_Sample.csv"
    raw_file.write_bytes(b"header\nrow\n")
    manifest = write_manifest(tmp_path / "manifest.toml", raw_file.name, file_sha256(raw_file))

    entry = resolve_source_file_entry(raw_file, FAMILY, manifest)

    assert entry.row_count == 1
    assert entry.retrieved_at == "2026-10-21"


def test_resolve_source_file_entry_rejects_republished_file(tmp_path: Path) -> None:
    raw_file = tmp_path / "202601_Sample.csv"
    raw_file.write_bytes(b"header\nchanged row\n")
    manifest = write_manifest(tmp_path / "manifest.toml", raw_file.name, "0" * 64)

    with pytest.raises(ValueError, match="republished"):
        resolve_source_file_entry(raw_file, FAMILY, manifest)


def test_resolve_source_file_entry_rejects_unregistered_file(tmp_path: Path) -> None:
    raw_file = tmp_path / "202602_Sample.csv"
    raw_file.write_bytes(b"header\n")
    manifest = write_manifest(tmp_path / "manifest.toml", "202601_Sample.csv", "0" * 64)

    with pytest.raises(ValueError, match="not registered"):
        resolve_source_file_entry(raw_file, FAMILY, manifest)


def test_repository_manifest_entries_are_well_formed() -> None:
    entries = load_source_manifest(Path("config/source_manifest.toml"))

    assert entries
    for entry in entries:
        assert len(entry.sha256) == 64
        assert entry.row_count > 0
        assert entry.retrieved_at
