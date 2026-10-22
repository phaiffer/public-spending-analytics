"""Registered raw source file versions, pinned by SHA-256."""

from __future__ import annotations

import hashlib
import tomllib
from dataclasses import dataclass
from pathlib import Path

DEFAULT_MANIFEST_PATH = Path("config/source_manifest.toml")
HASH_CHUNK_SIZE = 1 << 20


@dataclass(frozen=True)
class SourceFileEntry:
    """One registered version of an official raw file."""

    file_name: str
    source_family: str
    sha256: str
    row_count: int
    column_count: int
    reference_month: str | None = None
    download_page_url: str | None = None
    retrieved_at: str | None = None


def load_source_manifest(manifest_path: Path = DEFAULT_MANIFEST_PATH) -> list[SourceFileEntry]:
    """Load every registered file entry from the TOML manifest."""
    if not manifest_path.exists():
        raise FileNotFoundError(f"Source manifest not found: {manifest_path}")
    data = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    fields = SourceFileEntry.__dataclass_fields__
    return [
        SourceFileEntry(**{key: value for key, value in entry.items() if key in fields})
        for entry in data.get("files", [])
    ]


def file_sha256(file_path: Path) -> str:
    """Compute the hex SHA-256 digest of a file without loading it into memory."""
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        while chunk := handle.read(HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_source_file_entry(
    file_path: Path,
    source_family: str,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
) -> SourceFileEntry:
    """Return the manifest entry matching the file's name, family, and content hash."""
    candidates = [
        entry
        for entry in load_source_manifest(manifest_path)
        if entry.file_name == file_path.name and entry.source_family == source_family
    ]
    if not candidates:
        raise ValueError(
            f"Raw file {file_path.name} is not registered for {source_family} in {manifest_path}"
        )

    actual_sha256 = file_sha256(file_path)
    for entry in candidates:
        if entry.sha256 == actual_sha256:
            return entry
    raise ValueError(
        f"Raw file {file_path.name} has SHA-256 {actual_sha256}, which matches no registered "
        f"version in {manifest_path}. The source may have been republished; register the "
        "new version before staging it."
    )
