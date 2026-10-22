"""Keep local copies of the monthly Recebimentos archives in step with the Portal.

The Portal da Transparencia publishes one ZIP per month under despesas-favorecidos,
each holding a single YYYYMM_RecebimentosRecursosPorFavorecido.csv. It updates the
current month daily and republishes past months without notice, so there is no
fixed delay after which a month is final. Sync therefore asks the server for every
month's Last-Modified and size, downloads only what changed, and registers each new
content version (by SHA-256) in a local manifest before staging it.

Every download lands in a new folder and is treated as untrusted: the archive must
hold exactly the expected CSV, within a size limit, before anything is extracted.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import time
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from gov_spending_analytics.ingestion.source_manifest import (
    SourceFileEntry,
    append_source_file_entry,
    file_sha256,
    load_source_manifest,
)
from gov_spending_analytics.profiling.raw_csv import profile_raw_csv_file
from gov_spending_analytics.staging.portal_transparencia_recebimentos import (
    SOURCE_FAMILY,
    stage_recebimentos_recursos_por_favorecido_csv,
)

ARCHIVE_URL_TEMPLATE = (
    "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/"
    "despesas-favorecidos/{yyyymm}_Despesas.zip"
)
DOWNLOAD_PAGE_URL_TEMPLATE = (
    "https://portaldatransparencia.gov.br/download-de-dados/despesas-favorecidos/{yyyymm}"
)
# Earliest month published for this family (checked 2026-10-22).
FIRST_PUBLISHED_MONTH = "2013-12"
DEFAULT_LOOKBACK_MONTHS = 12

DEFAULT_SYNC_MANIFEST_PATH = Path("data/sync/recebimentos_source_manifest.toml")
DEFAULT_REMOTE_STATE_PATH = Path("data/sync/recebimentos_remote_state.json")
DEFAULT_DOWNLOAD_ROOT = Path("data/raw/portal_transparencia") / SOURCE_FAMILY
DEFAULT_STAGING_DIR = Path("data/staging/portal_transparencia") / SOURCE_FAMILY

# One month is a few hundred MB uncompressed at most; anything far larger is refused.
MAX_EXTRACTED_BYTES = 2 * 1024**3
DOWNLOAD_CHUNK_SIZE = 1 << 20
REQUEST_TIMEOUT_SECONDS = 60
REQUEST_ATTEMPTS = 3
# Pause between months. The download server sits behind a WAF that challenges
# clients sending many requests in a short window.
DEFAULT_REQUEST_INTERVAL_SECONDS = 3.0
USER_AGENT = "gov-spending-analytics (+https://github.com/phaiffer/public-spending-analytics)"

VERSION_MANIFEST_NAME = "source_entry.toml"
MANIFEST_HEADER = """\
# Recebimentos source file versions registered by `gov-spending sync-recebimentos`.
# Written by the sync job; one [[files]] entry per downloaded content version.
"""


@dataclass(frozen=True)
class RemoteArchive:
    """What the Portal server reports for one monthly archive."""

    reference_month: str
    url: str
    last_modified: str
    size_bytes: int


@dataclass(frozen=True)
class MonthSyncResult:
    """Outcome of syncing one month.

    status is one of: missing (not published), unchanged (server metadata unchanged),
    same_content (archive changed but the CSV is identical), staged (new version
    registered and staged), would_download (dry run), failed, blocked (the server
    rate limited the run, which stops there).
    """

    reference_month: str
    status: str
    detail: str = ""


@dataclass(frozen=True)
class SyncPaths:
    """Local locations used by one sync run."""

    manifest_path: Path = DEFAULT_SYNC_MANIFEST_PATH
    remote_state_path: Path = DEFAULT_REMOTE_STATE_PATH
    download_root: Path = DEFAULT_DOWNLOAD_ROOT
    staging_dir: Path = DEFAULT_STAGING_DIR


def parse_month(value: str) -> date:
    """Parse a YYYY-MM month key into the first day of that month."""
    try:
        return datetime.strptime(value, "%Y-%m").date()
    except ValueError as exc:
        raise ValueError(f"Expected a month as YYYY-MM, got {value!r}") from exc


def shift_month(month: str, offset: int) -> str:
    """Move a YYYY-MM month key by offset months."""
    first = parse_month(month)
    index = first.year * 12 + first.month - 1 + offset
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def month_range(start: str, end: str) -> list[str]:
    """Return every YYYY-MM month key from start to end, inclusive."""
    if parse_month(start) > parse_month(end):
        raise ValueError(f"Start month {start} is after end month {end}")
    months = [start]
    while months[-1] != end:
        months.append(shift_month(months[-1], 1))
    return months


def current_month(now: datetime | None = None) -> str:
    """Return the current UTC month as YYYY-MM."""
    return (now or datetime.now(UTC)).strftime("%Y-%m")


def select_months(
    include_all: bool,
    since: str | None,
    until: str | None,
    today_month: str,
) -> list[str]:
    """Resolve the months one run checks from the CLI options.

    The default is the current month and the DEFAULT_LOOKBACK_MONTHS before it,
    where the Portal makes most of its changes. Older republications are picked up
    by an occasional --all run.
    """
    if include_all:
        return month_range(FIRST_PUBLISHED_MONTH, until or today_month)
    if since:
        return month_range(since, until or today_month)
    if until:
        raise ValueError("--until needs --since or --all")
    return month_range(shift_month(today_month, -DEFAULT_LOOKBACK_MONTHS), today_month)


def archive_url(month: str, template: str = ARCHIVE_URL_TEMPLATE) -> str:
    """Build the archive URL for a YYYY-MM month key."""
    return template.format(yyyymm=month.replace("-", ""))


def csv_member_name(month: str) -> str:
    """Name of the CSV the Portal ships inside the monthly archive."""
    return f"{month.replace('-', '')}_RecebimentosRecursosPorFavorecido.csv"


def http_date_to_iso(value: str | None) -> str:
    """Convert an HTTP date header to ISO 8601 UTC, or return an empty string."""
    if not value:
        return ""
    return parsedate_to_datetime(value).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class PortalBlockedError(RuntimeError):
    """The download server answered with a bot challenge or a rate limit."""


def is_blocked_response(exc: HTTPError) -> bool:
    """True for a rate limit or an AWS WAF challenge (served as 405 with a WAF header)."""
    return exc.code == 429 or bool(exc.headers and exc.headers.get("x-amzn-waf-action"))


def open_with_retries(request: Request, timeout: int = REQUEST_TIMEOUT_SECONDS):
    """Open a request, retrying transient network and server errors.

    A rate limit or bot challenge is never retried: it raises PortalBlockedError so
    the run stops instead of adding load to a server that is pushing back.
    """
    for attempt in range(1, REQUEST_ATTEMPTS + 1):
        try:
            return urlopen(request, timeout=timeout)
        except HTTPError as exc:
            if is_blocked_response(exc):
                raise PortalBlockedError(
                    f"The Portal download server is rate limiting this machine "
                    f"(HTTP {exc.code}, WAF action {exc.headers.get('x-amzn-waf-action')!r}). "
                    "Stopped; wait a few hours before the next run."
                ) from exc
            if exc.code < 500 or attempt == REQUEST_ATTEMPTS:
                raise
        except URLError:
            if attempt == REQUEST_ATTEMPTS:
                raise
        time.sleep(2**attempt)
    raise RuntimeError("unreachable")


def head_archive(month: str, template: str = ARCHIVE_URL_TEMPLATE) -> RemoteArchive | None:
    """Ask the server for one month's archive metadata; None when it is not published."""
    url = archive_url(month, template)
    request = Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    try:
        with open_with_retries(request) as response:
            return RemoteArchive(
                reference_month=month,
                url=url,
                last_modified=http_date_to_iso(response.headers.get("Last-Modified")),
                size_bytes=int(response.headers.get("Content-Length") or 0),
            )
    except HTTPError as exc:
        # The download server answers 403 or 404 for months it does not publish;
        # rate limits and WAF challenges were already raised as PortalBlockedError.
        if exc.code in (403, 404):
            return None
        raise


def download_archive(remote: RemoteArchive, destination: Path) -> tuple[RemoteArchive, str]:
    """Stream an archive to destination; return the metadata seen on GET and its SHA-256."""
    request = Request(remote.url, headers={"User-Agent": USER_AGENT})
    digest = hashlib.sha256()
    with open_with_retries(request) as response, destination.open("wb") as handle:
        while chunk := response.read(DOWNLOAD_CHUNK_SIZE):
            digest.update(chunk)
            handle.write(chunk)
        last_modified = http_date_to_iso(response.headers.get("Last-Modified"))
    fetched = RemoteArchive(
        reference_month=remote.reference_month,
        url=remote.url,
        last_modified=last_modified or remote.last_modified,
        size_bytes=destination.stat().st_size,
    )
    return fetched, digest.hexdigest()


def extract_expected_csv(archive_path: Path, member_name: str, destination_dir: Path) -> Path:
    """Extract the single expected CSV from an untrusted archive."""
    with zipfile.ZipFile(archive_path) as archive:
        members = archive.infolist()
        names = [member.filename for member in members]
        if names != [member_name]:
            raise ValueError(f"{archive_path.name} should hold only {member_name}, got {names}")
        if members[0].file_size > MAX_EXTRACTED_BYTES:
            raise ValueError(f"{member_name} declares {members[0].file_size} bytes, over the limit")
        output_path = destination_dir / member_name
        copied = 0
        with archive.open(members[0]) as source, output_path.open("wb") as target:
            while chunk := source.read(DOWNLOAD_CHUNK_SIZE):
                copied += len(chunk)
                if copied > MAX_EXTRACTED_BYTES:
                    raise ValueError(f"{member_name} exceeds the extraction size limit")
                target.write(chunk)
    return output_path


def load_remote_state(state_path: Path) -> dict[str, dict]:
    """Load the last server metadata seen per month."""
    if not state_path.exists():
        return {}
    return json.loads(state_path.read_text(encoding="utf-8"))


def save_remote_state(state_path: Path, state: dict[str, dict]) -> None:
    """Persist the server metadata seen per month."""
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = state_path.with_name(state_path.name + ".partial")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(state_path)


def latest_entries_by_month(manifest_path: Path) -> dict[str, SourceFileEntry]:
    """Return the most recently registered entry for each reference month."""
    if not manifest_path.exists():
        return {}
    latest: dict[str, SourceFileEntry] = {}
    for entry in load_source_manifest(manifest_path):
        if entry.reference_month:
            latest[entry.reference_month] = entry
    return latest


def server_metadata_changed(remote: RemoteArchive, seen: dict | None) -> bool:
    """True when the server reports different metadata than the last run saw."""
    if seen is None:
        return True
    return (
        remote.last_modified != seen.get("archive_last_modified")
        or remote.size_bytes != seen.get("archive_size_bytes")
    )


def sync_recebimentos(
    months: list[str],
    paths: SyncPaths | None = None,
    archive_url_template: str = ARCHIVE_URL_TEMPLATE,
    keep_csv: bool = False,
    dry_run: bool = False,
    now: datetime | None = None,
    report: Callable[[MonthSyncResult], None] | None = None,
    request_interval: float = DEFAULT_REQUEST_INTERVAL_SECONDS,
) -> list[MonthSyncResult]:
    """Check each month on the Portal and stage every new content version.

    Months are checked one at a time, request_interval seconds apart. A failure in
    one month is reported and the run moves on; the failed month is retried on the
    next run because its server state is not saved. A rate limit or bot challenge
    stops the whole run.
    """
    paths = paths or SyncPaths()
    now = now or datetime.now(UTC)
    remote_state = load_remote_state(paths.remote_state_path)
    latest = latest_entries_by_month(paths.manifest_path)

    results = []
    for index, month in enumerate(months):
        if index and request_interval > 0:
            time.sleep(request_interval)
        try:
            result = sync_month(
                month=month,
                paths=paths,
                remote_state=remote_state,
                latest_entry=latest.get(month),
                archive_url_template=archive_url_template,
                keep_csv=keep_csv,
                dry_run=dry_run,
                now=now,
            )
        except PortalBlockedError as exc:
            result = MonthSyncResult(month, "blocked", str(exc))
        except (OSError, ValueError, RuntimeError, zipfile.BadZipFile) as exc:
            result = MonthSyncResult(month, "failed", str(exc))
        if not dry_run:
            save_remote_state(paths.remote_state_path, remote_state)
        if report:
            report(result)
        results.append(result)
        if result.status == "blocked":
            break
    return results


def sync_month(
    month: str,
    paths: SyncPaths,
    remote_state: dict[str, dict],
    latest_entry: SourceFileEntry | None,
    archive_url_template: str,
    keep_csv: bool,
    dry_run: bool,
    now: datetime,
) -> MonthSyncResult:
    """Sync one month; records the server metadata in remote_state once handled."""
    remote = head_archive(month, archive_url_template)
    if remote is None:
        return MonthSyncResult(month, "missing", "not published on the Portal")

    staged_path = paths.staging_dir / f"{Path(csv_member_name(month)).stem}.parquet"
    if not server_metadata_changed(remote, remote_state.get(month)) and staged_path.exists():
        return MonthSyncResult(month, "unchanged", f"last modified {remote.last_modified}")
    if dry_run:
        return MonthSyncResult(month, "would_download", f"last modified {remote.last_modified}")

    version_dir = paths.download_root / month.replace("-", "") / now.strftime("%Y%m%dT%H%M%SZ")
    version_dir.mkdir(parents=True, exist_ok=False)
    fetched, archive_sha256 = download_archive(remote, version_dir / Path(remote.url).name)
    csv_path = extract_expected_csv(
        version_dir / Path(remote.url).name, csv_member_name(month), version_dir
    )
    csv_sha256 = file_sha256(csv_path)

    if latest_entry is not None and latest_entry.sha256 == csv_sha256 and staged_path.exists():
        result = MonthSyncResult(month, "same_content", f"SHA-256 {csv_sha256[:12]} already staged")
        shutil.rmtree(version_dir)
    else:
        entry = register_and_stage(
            csv_path=csv_path,
            csv_sha256=csv_sha256,
            fetched=fetched,
            archive_sha256=archive_sha256,
            paths=paths,
            staged_path=staged_path,
            now=now,
        )
        detail = f"{entry.row_count} rows, SHA-256 {csv_sha256[:12]}"
        result = MonthSyncResult(month, "staged", detail)
        if not keep_csv:
            csv_path.unlink()

    remote_state[month] = {
        "archive_last_modified": fetched.last_modified,
        "archive_size_bytes": fetched.size_bytes,
        "archive_sha256": archive_sha256,
        "checked_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    return result


def register_and_stage(
    csv_path: Path,
    csv_sha256: str,
    fetched: RemoteArchive,
    archive_sha256: str,
    paths: SyncPaths,
    staged_path: Path,
    now: datetime,
) -> SourceFileEntry:
    """Profile and stage a new CSV version, then register it in the sync manifest.

    Staging runs against a one-entry manifest next to the download, so the shared
    manifest only gains versions that staged successfully.
    """
    month = fetched.reference_month
    version_dir = csv_path.parent
    profile_path = profile_raw_csv_file(
        file_path=csv_path,
        output_path=version_dir / f"{csv_path.stem}_profile.json",
        sample_size=0,
    )
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    entry = SourceFileEntry(
        file_name=csv_path.name,
        source_family=SOURCE_FAMILY,
        sha256=csv_sha256,
        row_count=profile["row_count"],
        column_count=profile["column_count"],
        reference_month=month,
        download_page_url=DOWNLOAD_PAGE_URL_TEMPLATE.format(yyyymm=month.replace("-", "")),
        retrieved_at=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        size_bytes=csv_path.stat().st_size,
        archive_url=fetched.url,
        archive_sha256=archive_sha256,
        archive_last_modified=fetched.last_modified,
        archive_size_bytes=fetched.size_bytes,
        is_open_month=month >= current_month(now),
    )
    version_manifest = version_dir / VERSION_MANIFEST_NAME
    append_source_file_entry(version_manifest, entry)
    stage_version(csv_path, profile_path, version_manifest, staged_path)
    append_source_file_entry(paths.manifest_path, entry, header=MANIFEST_HEADER)
    return entry


def stage_version(
    csv_path: Path, profile_path: Path, version_manifest: Path, staged_path: Path
) -> None:
    """Stage one registered CSV version, replacing the month's Parquet atomically."""
    staged_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = staged_path.with_name(staged_path.name + ".partial")
    stage_recebimentos_recursos_por_favorecido_csv(
        file_path=csv_path,
        profile_path=profile_path,
        output_path=partial_path,
        manifest_path=version_manifest,
    )
    partial_path.replace(staged_path)


def version_dir_for(entry: SourceFileEntry, download_root: Path) -> Path:
    """Folder where sync stored the download of a registered version."""
    retrieved = datetime.strptime(entry.retrieved_at or "", "%Y-%m-%dT%H:%M:%SZ")
    month_folder = (entry.reference_month or "").replace("-", "")
    return download_root / month_folder / retrieved.strftime("%Y%m%dT%H%M%SZ")


def restage_from_archives(
    paths: SyncPaths | None = None,
    report: Callable[[MonthSyncResult], None] | None = None,
) -> list[MonthSyncResult]:
    """Restage the latest registered version of every month from the kept archives.

    Used after a staging change; nothing is requested from the Portal. The CSV is
    re-extracted and must still match the registered SHA-256.
    """
    paths = paths or SyncPaths()
    results = []
    for month, entry in sorted(latest_entries_by_month(paths.manifest_path).items()):
        try:
            version_dir = version_dir_for(entry, paths.download_root)
            archive_path = version_dir / Path(entry.archive_url or "").name
            csv_path = extract_expected_csv(archive_path, entry.file_name, version_dir)
            try:
                stage_version(
                    csv_path=csv_path,
                    profile_path=version_dir / f"{csv_path.stem}_profile.json",
                    version_manifest=version_dir / VERSION_MANIFEST_NAME,
                    staged_path=paths.staging_dir / f"{csv_path.stem}.parquet",
                )
            finally:
                csv_path.unlink()
            result = MonthSyncResult(month, "staged", f"{entry.row_count} rows from {version_dir}")
        except (OSError, ValueError, RuntimeError, zipfile.BadZipFile) as exc:
            result = MonthSyncResult(month, "failed", str(exc))
        if report:
            report(result)
        results.append(result)
    return results


def count_by_status(results: list[MonthSyncResult]) -> dict[str, int]:
    """Count results per status."""
    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    return counts
