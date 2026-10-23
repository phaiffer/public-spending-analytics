"""Sync against a local HTTP server that mimics the Portal download server."""

from __future__ import annotations

import os
import threading
import zipfile
from collections.abc import Iterator
from datetime import UTC, datetime
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gov_spending_analytics.ingestion.portal_sync import (
    MonthSyncResult,
    SyncPaths,
    load_remote_state,
    month_range,
    restage_from_archives,
    select_months,
    shift_month,
    sync_recebimentos,
)
from gov_spending_analytics.ingestion.source_manifest import load_source_manifest

SAMPLE_CSV = (
    Path(__file__).parent
    / "fixtures"
    / "recebimentos_sample"
    / "202601_RecebimentosRecursosPorFavorecido.csv"
)
SAMPLE_ROW_COUNT = 17
NOW = datetime(2026, 10, 22, 12, 0, tzinfo=UTC)


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        pass


class WafChallengeHandler(QuietHandler):
    """Answers like the Portal's CloudFront WAF once it flags a client."""

    def do_HEAD(self) -> None:
        self.send_response(405)
        self.send_header("x-amzn-waf-action", "captcha")
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_GET = do_HEAD


def serve(directory: Path, handler: type[SimpleHTTPRequestHandler]) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("127.0.0.1", 0), partial(handler, directory=str(directory)))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


@pytest.fixture
def portal(tmp_path: Path) -> Iterator[tuple[Path, str]]:
    """Serve tmp_path/portal over HTTP; yield the folder and the archive URL template."""
    portal_dir = tmp_path / "portal"
    portal_dir.mkdir()
    server = serve(portal_dir, QuietHandler)
    try:
        yield portal_dir, f"http://127.0.0.1:{server.server_port}/{{yyyymm}}_Despesas.zip"
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def paths(tmp_path: Path) -> SyncPaths:
    return SyncPaths(
        manifest_path=tmp_path / "sync" / "manifest.toml",
        remote_state_path=tmp_path / "sync" / "state.json",
        download_root=tmp_path / "raw",
        staging_dir=tmp_path / "staging",
    )


def publish(portal_dir: Path, csv_bytes: bytes, mtime: int, extra_member: bool = False) -> None:
    """Write the 2026-01 archive the way the Portal ships it, with a given Last-Modified."""
    archive_path = portal_dir / "202601_Despesas.zip"
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(SAMPLE_CSV.name, csv_bytes)
        if extra_member:
            archive.writestr("../escape.txt", b"not expected")
    os.utime(archive_path, (mtime, mtime))


def run_sync(
    paths: SyncPaths, template: str, months: list[str], **options: object
) -> dict[str, MonthSyncResult]:
    options.setdefault("now", NOW)
    results = sync_recebimentos(
        months, paths, archive_url_template=template, request_interval=0, **options
    )
    return {result.reference_month: result for result in results}


def statuses(results: dict[str, MonthSyncResult]) -> dict[str, str]:
    return {month: result.status for month, result in results.items()}


def staged_rows(paths: SyncPaths) -> pd.DataFrame:
    return pd.read_parquet(paths.staging_dir / "202601_RecebimentosRecursosPorFavorecido.parquet")


def test_month_helpers() -> None:
    assert month_range("2025-11", "2026-02") == ["2025-11", "2025-12", "2026-01", "2026-02"]
    assert shift_month("2026-01", -1) == "2025-12"
    default = select_months(False, None, None, "2026-10")
    assert (default[0], default[-1], len(default)) == ("2025-10", "2026-10", 13)
    assert select_months(False, "2026-08", None, "2026-10") == ["2026-08", "2026-09", "2026-10"]
    assert select_months(True, None, "2014-01", "2026-10") == ["2013-12", "2014-01"]
    with pytest.raises(ValueError):
        select_months(False, None, "2026-01", "2026-10")


def test_sync_stages_a_new_month_then_skips_it_while_unchanged(
    portal: tuple[Path, str], paths: SyncPaths
) -> None:
    portal_dir, template = portal
    publish(portal_dir, SAMPLE_CSV.read_bytes(), mtime=1_760_000_000)

    first = run_sync(paths, template, ["2025-12", "2026-01"])

    assert statuses(first) == {"2025-12": "missing", "2026-01": "staged"}
    [entry] = load_source_manifest(paths.manifest_path)
    assert entry.row_count == SAMPLE_ROW_COUNT
    assert entry.reference_month == "2026-01"
    assert entry.is_open_month is False
    assert entry.archive_last_modified == "2025-10-09T08:53:20Z"
    staged = staged_rows(paths)
    assert len(staged) == SAMPLE_ROW_COUNT
    assert set(staged["source_registered_row_count"]) == {SAMPLE_ROW_COUNT}
    assert not list(paths.download_root.rglob("*.csv")), "CSV is removed after staging"
    assert list(paths.download_root.rglob("*_Despesas.zip")), "archive is kept"

    second = run_sync(paths, template, ["2026-01"], now=NOW.replace(hour=13))
    assert statuses(second) == {"2026-01": "unchanged"}


def test_sync_registers_a_republished_version(
    portal: tuple[Path, str], paths: SyncPaths
) -> None:
    portal_dir, template = portal
    sample = SAMPLE_CSV.read_bytes()
    publish(portal_dir, sample, mtime=1_760_000_000)
    run_sync(paths, template, ["2026-01"])

    # Same CSV inside a regenerated archive: no new version.
    publish(portal_dir, sample, mtime=1_760_100_000)
    rezipped = run_sync(paths, template, ["2026-01"], now=NOW.replace(hour=13))
    assert statuses(rezipped) == {"2026-01": "same_content"}
    assert len(load_source_manifest(paths.manifest_path)) == 1

    # The Portal drops the last row: a new version is registered and restaged.
    republished = sample[: sample.rstrip(b"\r\n").rfind(b"\r\n") + 2]
    publish(portal_dir, republished, mtime=1_760_200_000)
    result = run_sync(paths, template, ["2026-01"], now=NOW.replace(hour=14))

    assert statuses(result) == {"2026-01": "staged"}
    entries = load_source_manifest(paths.manifest_path)
    assert [entry.row_count for entry in entries] == [SAMPLE_ROW_COUNT, SAMPLE_ROW_COUNT - 1]
    assert len(staged_rows(paths)) == SAMPLE_ROW_COUNT - 1


def test_sync_refuses_an_archive_with_unexpected_members(
    portal: tuple[Path, str], paths: SyncPaths
) -> None:
    portal_dir, template = portal
    publish(portal_dir, SAMPLE_CSV.read_bytes(), mtime=1_760_000_000, extra_member=True)

    result = run_sync(paths, template, ["2026-01"])["2026-01"]

    assert result.status == "failed"
    assert "should hold only" in result.detail
    assert "2026-01" not in load_remote_state(paths.remote_state_path)
    assert not paths.manifest_path.exists()
    assert not (paths.staging_dir / "202601_RecebimentosRecursosPorFavorecido.parquet").exists()
    assert not list(paths.download_root.rglob("*.*")), "a failed version leaves no files behind"


def test_sync_stops_when_the_server_challenges_the_client(
    tmp_path: Path, paths: SyncPaths
) -> None:
    server = serve(tmp_path, WafChallengeHandler)
    template = f"http://127.0.0.1:{server.server_port}/{{yyyymm}}_Despesas.zip"
    try:
        results = run_sync(paths, template, ["2025-12", "2026-01", "2026-02"])
    finally:
        server.shutdown()
        server.server_close()

    assert statuses(results) == {"2025-12": "blocked"}, "no further month is requested"
    assert "rate limiting" in results["2025-12"].detail
    assert load_remote_state(paths.remote_state_path) == {}


def test_dry_run_downloads_nothing(portal: tuple[Path, str], paths: SyncPaths) -> None:
    portal_dir, template = portal
    publish(portal_dir, SAMPLE_CSV.read_bytes(), mtime=1_760_000_000)

    result = run_sync(paths, template, ["2026-01"], dry_run=True)["2026-01"]

    assert result.status == "would_download"
    assert not paths.download_root.exists()
    assert not paths.remote_state_path.exists()


def test_restage_rebuilds_months_from_kept_archives(
    portal: tuple[Path, str], paths: SyncPaths
) -> None:
    portal_dir, template = portal
    publish(portal_dir, SAMPLE_CSV.read_bytes(), mtime=1_760_000_000)
    run_sync(paths, template, ["2026-01"])
    staged_file = paths.staging_dir / "202601_RecebimentosRecursosPorFavorecido.parquet"
    staged_file.unlink()
    (portal_dir / "202601_Despesas.zip").unlink()  # the Portal is not contacted

    [result] = restage_from_archives(paths)

    assert result.status == "staged"
    assert len(staged_rows(paths)) == SAMPLE_ROW_COUNT
    amount_type = pq.read_schema(staged_file).field("amount_received_brl").type
    assert amount_type == pa.decimal128(18, 4)
    assert not list(paths.download_root.rglob("*.csv"))


def test_current_month_is_registered_as_open(portal: tuple[Path, str], paths: SyncPaths) -> None:
    portal_dir, template = portal
    publish(portal_dir, SAMPLE_CSV.read_bytes(), mtime=1_760_000_000)

    run_sync(paths, template, ["2026-01"], now=datetime(2026, 1, 20, tzinfo=UTC))

    [entry] = load_source_manifest(paths.manifest_path)
    assert entry.is_open_month is True
