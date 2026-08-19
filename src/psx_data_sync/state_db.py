"""Versioned SQLite persistence for synchronization metadata.

The repository opens a short-lived connection per operation. Async callers use
``AsyncStateRepository`` to move those small operations off the event loop and
serialize writes with one lock; network work remains fully concurrent.
"""

from __future__ import annotations

import asyncio
import os
import re
import sqlite3
import time
import uuid
from collections import Counter
from collections.abc import Iterable
from datetime import date, datetime, timezone
from pathlib import Path

from . import __version__
from .config import CANONICAL_COLUMNS, Settings
from .exporter import inspect_existing_canonical_file
from .state import (
    BootstrapFileResult,
    BootstrapResult,
    DateSyncState,
    DownloadAttemptEvent,
    DownloadAttemptRecord,
    DownloadResult,
    DownloadStatus,
    PersistentSyncStatus,
    StateSummary,
    SyncEvidenceState,
    SyncRunRecord,
    SyncRunStatus,
)


SCHEMA_VERSION = 1
MARKET_FILE_PATTERN = re.compile(r"^market_(\d{4}-\d{2}-\d{2})\.csv$")
VERIFIED_STATUSES = frozenset(
    {
        PersistentSyncStatus.VERIFIED_TRADING_DATA,
        PersistentSyncStatus.ALREADY_PRESENT_VERIFIED,
    }
)
TRANSIENT_STATUSES = frozenset(
    {
        PersistentSyncStatus.EMPTY_UNRESOLVED,
        PersistentSyncStatus.TEMPORARY_FAILURE,
        PersistentSyncStatus.HTTP_FAILURE,
        PersistentSyncStatus.PARSE_FAILURE,
        PersistentSyncStatus.VALIDATION_FAILURE,
    }
)
EXPECTED_TABLES = frozenset(
    {
        "sync_schema_metadata",
        "date_sync_state",
        "sync_runs",
        "download_attempts",
        "sync_run_date_results",
    }
)
EXPECTED_INDEXES = frozenset(
    {
        "idx_date_sync_state_status",
        "idx_download_attempts_market_date",
        "idx_download_attempts_run_id",
        "idx_sync_run_date_results_market_date",
        "idx_sync_runs_started_at",
    }
)


class StateDatabaseError(RuntimeError):
    """Base persistence-layer error."""


class IncompatibleSchemaError(StateDatabaseError):
    """Raised instead of silently changing an unknown schema version."""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def persistent_status_for_download(status: DownloadStatus) -> PersistentSyncStatus:
    mapping = {
        DownloadStatus.TRADING_DATA: PersistentSyncStatus.VERIFIED_TRADING_DATA,
        DownloadStatus.ALREADY_PRESENT: PersistentSyncStatus.ALREADY_PRESENT_VERIFIED,
        DownloadStatus.EMPTY_MARKET_RESPONSE: PersistentSyncStatus.EMPTY_UNRESOLVED,
        DownloadStatus.NON_TRADING_OR_EMPTY: PersistentSyncStatus.EMPTY_UNRESOLVED,
        DownloadStatus.TEMPORARY_FAILURE: PersistentSyncStatus.TEMPORARY_FAILURE,
        DownloadStatus.HTTP_FAILURE: PersistentSyncStatus.HTTP_FAILURE,
        DownloadStatus.PARSE_FAILURE: PersistentSyncStatus.PARSE_FAILURE,
        DownloadStatus.VALIDATION_FAILURE: PersistentSyncStatus.VALIDATION_FAILURE,
        DownloadStatus.FILE_CONFLICT: PersistentSyncStatus.FILE_CONFLICT,
        DownloadStatus.EXISTING_FILE_INVALID: PersistentSyncStatus.FILE_CORRUPT,
        DownloadStatus.SAVE_FAILURE: PersistentSyncStatus.TEMPORARY_FAILURE,
        DownloadStatus.INVALID_DATE: PersistentSyncStatus.NEVER_ATTEMPTED,
    }
    return mapping[status]


def resolve_status_transition(
    previous: PersistentSyncStatus,
    incoming: PersistentSyncStatus,
) -> PersistentSyncStatus:
    """Apply the only permitted current-state transition policy."""

    if incoming is PersistentSyncStatus.VERIFIED_TRADING_DATA:
        return incoming
    if incoming is PersistentSyncStatus.ALREADY_PRESENT_VERIFIED:
        return previous if previous in VERIFIED_STATUSES else incoming
    if previous in VERIFIED_STATUSES and incoming in TRANSIENT_STATUSES:
        return previous
    if (
        previous is PersistentSyncStatus.HTTP_FAILURE
        and incoming is PersistentSyncStatus.TEMPORARY_FAILURE
    ):
        return previous
    return incoming


def _clean_error(message: str | None) -> str | None:
    if message is None:
        return None
    return " ".join(message.split())[:1000]


class StateRepository:
    """Encapsulate all SQL and state transitions for one SQLite database."""

    def __init__(
        self,
        database_path: Path,
        *,
        project_root: Path | None = None,
        source_endpoint: str = Settings().historical_url,
        application_version: str = __version__,
    ) -> None:
        self.database_path = Path(database_path)
        self.project_root = (project_root or Path.cwd()).resolve()
        self.source_endpoint = source_endpoint
        self.application_version = application_version

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def initialize(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_schema_metadata (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    schema_version INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    application_version TEXT NOT NULL
                )
                """
            )
            existing = connection.execute(
                "SELECT schema_version FROM sync_schema_metadata WHERE singleton = 1"
            ).fetchone()
            if existing is not None and existing["schema_version"] != SCHEMA_VERSION:
                raise IncompatibleSchemaError(
                    "unsupported sync schema version "
                    f"{existing['schema_version']}; expected {SCHEMA_VERSION}"
                )

            now = utc_now_iso()
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO sync_schema_metadata (
                        singleton, schema_version, created_at, updated_at,
                        application_version
                    ) VALUES (1, ?, ?, ?, ?)
                    """,
                    (SCHEMA_VERSION, now, now, self.application_version),
                )

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS date_sync_state (
                    market_date TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    evidence_state TEXT NOT NULL DEFAULT 'NONE',
                    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
                    successful_attempt_count INTEGER NOT NULL DEFAULT 0
                        CHECK (successful_attempt_count >= 0),
                    last_http_status INTEGER,
                    last_response_bytes INTEGER NOT NULL DEFAULT 0
                        CHECK (last_response_bytes >= 0),
                    parsed_row_count INTEGER NOT NULL DEFAULT 0
                        CHECK (parsed_row_count >= 0),
                    valid_row_count INTEGER NOT NULL DEFAULT 0
                        CHECK (valid_row_count >= 0),
                    rejected_row_count INTEGER NOT NULL DEFAULT 0
                        CHECK (rejected_row_count >= 0),
                    csv_checksum_sha256 TEXT,
                    csv_relative_path TEXT,
                    first_attempt_at TEXT,
                    last_attempt_at TEXT,
                    last_success_at TEXT,
                    last_verified_at TEXT,
                    last_error_type TEXT,
                    last_error_message TEXT,
                    last_duration_ms REAL,
                    source_endpoint TEXT NOT NULL,
                    record_created_at TEXT NOT NULL,
                    record_updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_runs (
                    run_id TEXT PRIMARY KEY,
                    command_type TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    requested_date_count INTEGER NOT NULL,
                    worker_count INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    duration_ms REAL,
                    completed_count INTEGER NOT NULL DEFAULT 0,
                    network_fetch_count INTEGER NOT NULL DEFAULT 0,
                    local_skip_count INTEGER NOT NULL DEFAULT 0,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    unresolved_count INTEGER NOT NULL DEFAULT 0,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    total_valid_rows INTEGER NOT NULL DEFAULT 0,
                    total_rejected_rows INTEGER NOT NULL DEFAULT 0,
                    total_response_bytes INTEGER NOT NULL DEFAULT 0,
                    total_attempts INTEGER NOT NULL DEFAULT 0,
                    interrupted INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    application_version TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS download_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    market_date TEXT NOT NULL,
                    attempt_number INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    duration_ms REAL NOT NULL,
                    http_status INTEGER,
                    response_bytes INTEGER NOT NULL DEFAULT 0,
                    response_classification TEXT,
                    final_status TEXT NOT NULL,
                    retryable INTEGER NOT NULL DEFAULT 0,
                    error_type TEXT,
                    error_message TEXT,
                    parsed_row_count INTEGER NOT NULL DEFAULT 0,
                    valid_row_count INTEGER NOT NULL DEFAULT 0,
                    rejected_row_count INTEGER NOT NULL DEFAULT 0,
                    checksum TEXT,
                    csv_relative_path TEXT,
                    worker_identifier TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE (run_id, market_date, attempt_number),
                    FOREIGN KEY (run_id) REFERENCES sync_runs(run_id),
                    FOREIGN KEY (market_date) REFERENCES date_sync_state(market_date)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_run_date_results (
                    run_id TEXT NOT NULL,
                    market_date TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts_in_run INTEGER NOT NULL,
                    local_skip INTEGER NOT NULL DEFAULT 0,
                    parsed_row_count INTEGER NOT NULL DEFAULT 0,
                    valid_row_count INTEGER NOT NULL DEFAULT 0,
                    rejected_row_count INTEGER NOT NULL DEFAULT 0,
                    response_bytes INTEGER NOT NULL DEFAULT 0,
                    checksum TEXT,
                    csv_relative_path TEXT,
                    duration_ms REAL NOT NULL,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, market_date),
                    FOREIGN KEY (run_id) REFERENCES sync_runs(run_id),
                    FOREIGN KEY (market_date) REFERENCES date_sync_state(market_date)
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_date_sync_state_status "
                "ON date_sync_state(status)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_download_attempts_market_date "
                "ON download_attempts(market_date, created_at)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_download_attempts_run_id "
                "ON download_attempts(run_id)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_sync_run_date_results_market_date "
                "ON sync_run_date_results(market_date)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_sync_runs_started_at "
                "ON sync_runs(started_at)"
            )
            connection.execute(
                """
                UPDATE sync_schema_metadata
                SET updated_at = ?, application_version = ?
                WHERE singleton = 1
                """,
                (now, self.application_version),
            )

    def verify_schema(self) -> int:
        with self._connect() as connection:
            metadata = connection.execute(
                "SELECT schema_version FROM sync_schema_metadata WHERE singleton = 1"
            ).fetchone()
            if metadata is None or metadata["schema_version"] != SCHEMA_VERSION:
                found = None if metadata is None else metadata["schema_version"]
                raise IncompatibleSchemaError(
                    f"sync schema version {found!r} is incompatible; "
                    f"expected {SCHEMA_VERSION}"
                )
            tables = {
                row["name"]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            missing_tables = EXPECTED_TABLES - tables
            if missing_tables:
                raise StateDatabaseError(
                    "state database is missing tables: "
                    + ", ".join(sorted(missing_tables))
                )
            indexes = {
                row["name"]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'index'"
                )
            }
            missing_indexes = EXPECTED_INDEXES - indexes
            if missing_indexes:
                raise StateDatabaseError(
                    "state database is missing indexes: "
                    + ", ".join(sorted(missing_indexes))
                )
            return metadata["schema_version"]

    def database_settings(self) -> dict[str, object]:
        with self._connect() as connection:
            return {
                "foreign_keys": connection.execute(
                    "PRAGMA foreign_keys"
                ).fetchone()[0],
                "journal_mode": connection.execute(
                    "PRAGMA journal_mode"
                ).fetchone()[0],
                "synchronous": connection.execute(
                    "PRAGMA synchronous"
                ).fetchone()[0],
            }

    def schema_objects(self) -> tuple[set[str], set[str]]:
        with self._connect() as connection:
            tables = {
                row["name"]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            indexes = {
                row["name"]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'index'"
                )
            }
        return tables, indexes

    def _relative_path(self, path: Path | None) -> str | None:
        if path is None:
            return None
        absolute = path.resolve()
        return Path(os.path.relpath(absolute, self.project_root)).as_posix()

    def _ensure_date_state(
        self, connection: sqlite3.Connection, market_date: str, now: str
    ) -> None:
        connection.execute(
            """
            INSERT INTO date_sync_state (
                market_date, status, evidence_state, source_endpoint,
                record_created_at, record_updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(market_date) DO NOTHING
            """,
            (
                market_date,
                PersistentSyncStatus.NEVER_ATTEMPTED.value,
                SyncEvidenceState.NONE.value,
                self.source_endpoint,
                now,
                now,
            ),
        )

    @staticmethod
    def _state_from_row(row: sqlite3.Row) -> DateSyncState:
        return DateSyncState(
            market_date=row["market_date"],
            status=PersistentSyncStatus(row["status"]),
            evidence_state=SyncEvidenceState(row["evidence_state"]),
            attempt_count=row["attempt_count"],
            successful_attempt_count=row["successful_attempt_count"],
            last_http_status=row["last_http_status"],
            last_response_bytes=row["last_response_bytes"],
            parsed_row_count=row["parsed_row_count"],
            valid_row_count=row["valid_row_count"],
            rejected_row_count=row["rejected_row_count"],
            csv_checksum_sha256=row["csv_checksum_sha256"],
            csv_relative_path=row["csv_relative_path"],
            first_attempt_at=row["first_attempt_at"],
            last_attempt_at=row["last_attempt_at"],
            last_success_at=row["last_success_at"],
            last_verified_at=row["last_verified_at"],
            last_error_type=row["last_error_type"],
            last_error_message=row["last_error_message"],
            last_duration_ms=row["last_duration_ms"],
            source_endpoint=row["source_endpoint"],
            record_created_at=row["record_created_at"],
            record_updated_at=row["record_updated_at"],
        )

    def get_date_state(self, market_date: date | str) -> DateSyncState | None:
        date_text = market_date.isoformat() if isinstance(market_date, date) else market_date
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM date_sync_state WHERE market_date = ?", (date_text,)
            ).fetchone()
        return None if row is None else self._state_from_row(row)

    def begin_sync_run(
        self,
        command_type: str,
        start_date: str,
        end_date: str,
        requested_date_count: int,
        worker_count: int,
    ) -> str:
        run_id = uuid.uuid4().hex
        started_at = utc_now_iso()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO sync_runs (
                    run_id, command_type, start_date, end_date,
                    requested_date_count, worker_count, started_at,
                    interrupted, status, application_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                """,
                (
                    run_id,
                    command_type,
                    start_date,
                    end_date,
                    requested_date_count,
                    worker_count,
                    started_at,
                    SyncRunStatus.RUNNING.value,
                    self.application_version,
                ),
            )
        return run_id

    def record_attempt(self, run_id: str, event: DownloadAttemptEvent) -> None:
        now = utc_now_iso()
        relative_path = self._relative_path(event.saved_path)
        incoming = persistent_status_for_download(event.final_status)
        successful = event.final_status in {
            DownloadStatus.TRADING_DATA,
            DownloadStatus.ALREADY_PRESENT,
        }
        with self._connect() as connection:
            self._ensure_date_state(connection, event.requested_date, now)
            current_row = connection.execute(
                "SELECT * FROM date_sync_state WHERE market_date = ?",
                (event.requested_date,),
            ).fetchone()
            assert current_row is not None
            current = self._state_from_row(current_row)
            status = resolve_status_transition(current.status, incoming)
            preserve_verified_counts = (
                current.status in VERIFIED_STATUSES
                and incoming in TRANSIENT_STATUSES
                and status is current.status
            )
            parsed_row_count = (
                current.parsed_row_count
                if preserve_verified_counts
                else event.parsed_row_count
            )
            valid_row_count = (
                current.valid_row_count
                if preserve_verified_counts
                else event.valid_row_count
            )
            rejected_row_count = (
                current.rejected_row_count
                if preserve_verified_counts
                else event.rejected_row_count
            )
            evidence_state = (
                current.evidence_state
                if preserve_verified_counts
                else (
                    SyncEvidenceState.NETWORK_VALIDATED_CSV
                    if successful
                    else SyncEvidenceState.NETWORK_OBSERVATION
                )
            )
            error_type = None if successful else event.error_type
            error_message = None if successful else _clean_error(event.error_message)
            connection.execute(
                """
                UPDATE date_sync_state SET
                    status = ?,
                    evidence_state = ?,
                    attempt_count = attempt_count + 1,
                    successful_attempt_count = successful_attempt_count + ?,
                    last_http_status = ?,
                    last_response_bytes = ?,
                    parsed_row_count = ?,
                    valid_row_count = ?,
                    rejected_row_count = ?,
                    csv_checksum_sha256 = COALESCE(?, csv_checksum_sha256),
                    csv_relative_path = COALESCE(?, csv_relative_path),
                    first_attempt_at = COALESCE(first_attempt_at, ?),
                    last_attempt_at = ?,
                    last_success_at = CASE WHEN ? THEN ? ELSE last_success_at END,
                    last_verified_at = CASE WHEN ? THEN ? ELSE last_verified_at END,
                    last_error_type = ?,
                    last_error_message = ?,
                    last_duration_ms = ?,
                    source_endpoint = ?,
                    record_updated_at = ?
                WHERE market_date = ?
                """,
                (
                    status.value,
                    evidence_state.value,
                    int(successful),
                    event.http_status,
                    event.response_bytes,
                    parsed_row_count,
                    valid_row_count,
                    rejected_row_count,
                    event.checksum,
                    relative_path,
                    event.started_at,
                    event.finished_at,
                    int(successful),
                    event.finished_at,
                    int(successful),
                    event.finished_at,
                    error_type,
                    error_message,
                    event.duration_ms,
                    self.source_endpoint,
                    now,
                    event.requested_date,
                ),
            )
            connection.execute(
                """
                INSERT INTO download_attempts (
                    run_id, market_date, attempt_number, started_at, finished_at,
                    duration_ms, http_status, response_bytes,
                    response_classification, final_status, retryable,
                    error_type, error_message, parsed_row_count, valid_row_count,
                    rejected_row_count, checksum, csv_relative_path,
                    worker_identifier, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    event.requested_date,
                    event.attempt_number,
                    event.started_at,
                    event.finished_at,
                    event.duration_ms,
                    event.http_status,
                    event.response_bytes,
                    event.response_classification,
                    event.final_status.value,
                    int(event.retryable),
                    event.error_type,
                    _clean_error(event.error_message),
                    event.parsed_row_count,
                    event.valid_row_count,
                    event.rejected_row_count,
                    event.checksum,
                    relative_path,
                    event.worker_identifier,
                    now,
                ),
            )

    def record_download_result(self, run_id: str, result: DownloadResult) -> None:
        now = utc_now_iso()
        relative_path = self._relative_path(result.saved_path)
        incoming = persistent_status_for_download(result.status)
        successful = result.successful
        with self._connect() as connection:
            self._ensure_date_state(connection, result.requested_date, now)
            current_row = connection.execute(
                "SELECT * FROM date_sync_state WHERE market_date = ?",
                (result.requested_date,),
            ).fetchone()
            assert current_row is not None
            current = self._state_from_row(current_row)
            status = resolve_status_transition(current.status, incoming)
            preserve_verified_counts = (
                current.status in VERIFIED_STATUSES
                and incoming in TRANSIENT_STATUSES
                and status is current.status
            )
            parsed_row_count = (
                current.parsed_row_count
                if preserve_verified_counts
                else result.parsed_row_count
            )
            valid_row_count = (
                current.valid_row_count
                if preserve_verified_counts
                else result.valid_row_count
            )
            rejected_row_count = (
                current.rejected_row_count
                if preserve_verified_counts
                else result.rejected_row_count
            )
            if preserve_verified_counts:
                evidence_state = current.evidence_state
            elif successful:
                evidence_state = (
                    SyncEvidenceState.LOCAL_CSV_SHA256_VERIFIED
                    if result.locally_skipped
                    else SyncEvidenceState.NETWORK_VALIDATED_CSV
                )
            elif incoming is PersistentSyncStatus.FILE_CONFLICT:
                evidence_state = SyncEvidenceState.LOCAL_CHECKSUM_CONFLICT
            elif incoming is PersistentSyncStatus.FILE_CORRUPT:
                evidence_state = SyncEvidenceState.LOCAL_FILE_CORRUPT
            else:
                evidence_state = SyncEvidenceState.NETWORK_OBSERVATION
            source_endpoint = (
                current.source_endpoint
                if result.attempts == 0
                else self.source_endpoint
            )
            connection.execute(
                """
                UPDATE date_sync_state SET
                    status = ?,
                    evidence_state = ?,
                    last_http_status = COALESCE(?, last_http_status),
                    last_response_bytes = CASE WHEN ? > 0 THEN ? ELSE last_response_bytes END,
                    parsed_row_count = ?,
                    valid_row_count = ?,
                    rejected_row_count = ?,
                    csv_checksum_sha256 = COALESCE(?, csv_checksum_sha256),
                    csv_relative_path = COALESCE(?, csv_relative_path),
                    last_success_at = CASE
                        WHEN ? THEN COALESCE(last_success_at, ?) ELSE last_success_at END,
                    last_verified_at = CASE WHEN ? THEN ? ELSE last_verified_at END,
                    last_error_type = ?,
                    last_error_message = ?,
                    last_duration_ms = ?,
                    source_endpoint = ?,
                    record_updated_at = ?
                WHERE market_date = ?
                """,
                (
                    status.value,
                    evidence_state.value,
                    result.http_status,
                    result.response_bytes,
                    result.response_bytes,
                    parsed_row_count,
                    valid_row_count,
                    rejected_row_count,
                    result.checksum,
                    relative_path,
                    int(successful),
                    now,
                    int(successful),
                    now,
                    None if successful else result.status.value,
                    None if successful else _clean_error(result.error),
                    result.elapsed_ms,
                    source_endpoint,
                    now,
                    result.requested_date,
                ),
            )
            connection.execute(
                """
                INSERT INTO sync_run_date_results (
                    run_id, market_date, status, attempts_in_run, local_skip,
                    parsed_row_count, valid_row_count, rejected_row_count,
                    response_bytes, checksum, csv_relative_path, duration_ms,
                    error_message, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    result.requested_date,
                    result.status.value,
                    result.attempts,
                    int(result.locally_skipped),
                    result.parsed_row_count,
                    result.valid_row_count,
                    result.rejected_row_count,
                    result.transferred_response_bytes,
                    result.checksum,
                    relative_path,
                    result.elapsed_ms,
                    _clean_error(result.error),
                    now,
                ),
            )

    def _set_artifact_state(
        self,
        market_date: str,
        status: PersistentSyncStatus,
        *,
        row_count: int | None = None,
        checksum: str | None = None,
        path: Path | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        preserve_artifact: bool = False,
    ) -> bool:
        now = utc_now_iso()
        relative_path = self._relative_path(path)
        with self._connect() as connection:
            self._ensure_date_state(connection, market_date, now)
            row = connection.execute(
                "SELECT * FROM date_sync_state WHERE market_date = ?", (market_date,)
            ).fetchone()
            assert row is not None
            current = self._state_from_row(row)
            resolved = resolve_status_transition(current.status, status)
            new_rows = current.valid_row_count if row_count is None else row_count
            new_checksum = current.csv_checksum_sha256 if preserve_artifact else checksum
            new_path = current.csv_relative_path if preserve_artifact else relative_path
            cleaned_error = _clean_error(error_message)
            evidence_state = {
                PersistentSyncStatus.VERIFIED_TRADING_DATA: (
                    SyncEvidenceState.LOCAL_CSV_SHA256_VERIFIED
                ),
                PersistentSyncStatus.ALREADY_PRESENT_VERIFIED: (
                    SyncEvidenceState.LOCAL_CSV_SHA256_VERIFIED
                ),
                PersistentSyncStatus.FILE_MISSING: SyncEvidenceState.LOCAL_FILE_MISSING,
                PersistentSyncStatus.FILE_CORRUPT: SyncEvidenceState.LOCAL_FILE_CORRUPT,
                PersistentSyncStatus.FILE_CONFLICT: (
                    SyncEvidenceState.LOCAL_CHECKSUM_CONFLICT
                ),
            }.get(resolved, current.evidence_state)
            changed = any(
                (
                    current.status is not resolved,
                    current.evidence_state is not evidence_state,
                    current.valid_row_count != new_rows,
                    current.parsed_row_count != new_rows,
                    current.csv_checksum_sha256 != new_checksum,
                    current.csv_relative_path != new_path,
                    current.last_error_type != error_type,
                    current.last_error_message != cleaned_error,
                )
            )
            if not changed:
                return False
            verified = resolved in VERIFIED_STATUSES
            connection.execute(
                """
                UPDATE date_sync_state SET
                    status = ?, evidence_state = ?,
                    parsed_row_count = ?, valid_row_count = ?,
                    rejected_row_count = 0,
                    csv_checksum_sha256 = ?, csv_relative_path = ?,
                    last_success_at = CASE
                        WHEN ? THEN COALESCE(last_success_at, ?) ELSE last_success_at END,
                    last_verified_at = CASE WHEN ? THEN ? ELSE last_verified_at END,
                    last_error_type = ?, last_error_message = ?,
                    source_endpoint = ?, record_updated_at = ?
                WHERE market_date = ?
                """,
                (
                    resolved.value,
                    evidence_state.value,
                    new_rows,
                    new_rows,
                    new_checksum,
                    new_path,
                    int(verified),
                    now,
                    int(verified),
                    now,
                    error_type,
                    cleaned_error,
                    current.source_endpoint,
                    now,
                    market_date,
                ),
            )
            return True

    def prepare_fetch(
        self,
        market_date: date,
        output_dir: Path,
        columns: tuple[str, ...] = CANONICAL_COLUMNS,
    ) -> DownloadResult | None:
        """Central skip/corruption policy; return a local outcome or allow HTTP."""

        started = time.perf_counter()
        date_text = market_date.isoformat()
        current = self.get_date_state(date_text)
        inspection = inspect_existing_canonical_file(market_date, output_dir, columns)
        elapsed_ms = lambda: (time.perf_counter() - started) * 1000

        if inspection.valid:
            has_verified_identity = (
                current is not None
                and current.last_verified_at is not None
                and current.csv_checksum_sha256 is not None
            )
            if has_verified_identity:
                assert current is not None
                if current.csv_checksum_sha256 != inspection.checksum:
                    message = (
                        "verified database checksum differs from the valid canonical "
                        "CSV; automatic replacement is disabled"
                    )
                    self._set_artifact_state(
                        date_text,
                        PersistentSyncStatus.FILE_CONFLICT,
                        error_type="CHECKSUM_MISMATCH",
                        error_message=message,
                        preserve_artifact=True,
                    )
                    return DownloadResult(
                        requested_date=date_text,
                        status=DownloadStatus.FILE_CONFLICT,
                        elapsed_ms=elapsed_ms(),
                        error=message,
                    )
            self._set_artifact_state(
                date_text,
                PersistentSyncStatus.VERIFIED_TRADING_DATA,
                row_count=inspection.row_count,
                checksum=inspection.checksum,
                path=inspection.path,
            )
            return DownloadResult(
                requested_date=date_text,
                status=DownloadStatus.ALREADY_PRESENT,
                parsed_row_count=inspection.row_count,
                valid_row_count=inspection.row_count,
                elapsed_ms=elapsed_ms(),
                saved_path=inspection.path,
                checksum=inspection.checksum,
                locally_skipped=True,
                warnings=(
                    "persistent state and canonical CSV verified; PSX request skipped",
                ),
            )

        if inspection.exists:
            message = inspection.error or "existing CSV failed canonical validation"
            self._set_artifact_state(
                date_text,
                PersistentSyncStatus.FILE_CORRUPT,
                checksum=inspection.checksum,
                path=inspection.path,
                error_type="FILE_CORRUPT",
                error_message=message,
                preserve_artifact=(
                    current is not None
                    and current.last_verified_at is not None
                    and current.csv_checksum_sha256 is not None
                ),
            )
            return DownloadResult(
                requested_date=date_text,
                status=DownloadStatus.EXISTING_FILE_INVALID,
                elapsed_ms=elapsed_ms(),
                error=message,
            )

        if current is not None and current.status in VERIFIED_STATUSES:
            self._set_artifact_state(
                date_text,
                PersistentSyncStatus.FILE_MISSING,
                error_type="FILE_MISSING",
                error_message="verified CSV is missing; network recovery is allowed",
                preserve_artifact=True,
            )
        return None

    def bootstrap_local_files(self, output_dir: Path) -> BootstrapResult:
        outcomes: list[BootstrapFileResult] = []
        for path in sorted(output_dir.glob("market_*.csv")):
            match = MARKET_FILE_PATTERN.fullmatch(path.name)
            if match is None:
                outcomes.append(
                    BootstrapFileResult(
                        path=path,
                        market_date=None,
                        status=PersistentSyncStatus.FILE_CORRUPT,
                        row_count=0,
                        checksum=None,
                        changed=False,
                        error="filename does not contain a strict ISO market date",
                    )
                )
                continue
            date_text = match.group(1)
            try:
                market_date = date.fromisoformat(date_text)
            except ValueError:
                outcomes.append(
                    BootstrapFileResult(
                        path=path,
                        market_date=date_text,
                        status=PersistentSyncStatus.FILE_CORRUPT,
                        row_count=0,
                        checksum=None,
                        changed=False,
                        error="filename contains an invalid calendar date",
                    )
                )
                continue

            inspection = inspect_existing_canonical_file(market_date, output_dir)
            if inspection.valid:
                current = self.get_date_state(date_text)
                has_verified_identity = (
                    current is not None
                    and current.last_verified_at is not None
                    and current.csv_checksum_sha256 is not None
                )
                if (
                    has_verified_identity
                    and current is not None
                    and current.csv_checksum_sha256 != inspection.checksum
                ):
                    message = (
                        "verified database checksum differs from the valid "
                        "canonical CSV; bootstrap did not replace its identity"
                    )
                    changed = self._set_artifact_state(
                        date_text,
                        PersistentSyncStatus.FILE_CONFLICT,
                        error_type="CHECKSUM_MISMATCH",
                        error_message=message,
                        preserve_artifact=True,
                    )
                    outcome_status = PersistentSyncStatus.FILE_CONFLICT
                    outcome_error = message
                else:
                    changed = self._set_artifact_state(
                        date_text,
                        PersistentSyncStatus.VERIFIED_TRADING_DATA,
                        row_count=inspection.row_count,
                        checksum=inspection.checksum,
                        path=inspection.path,
                    )
                    outcome_status = PersistentSyncStatus.VERIFIED_TRADING_DATA
                    outcome_error = None
                outcomes.append(
                    BootstrapFileResult(
                        path=path,
                        market_date=date_text,
                        status=outcome_status,
                        row_count=inspection.row_count,
                        checksum=inspection.checksum,
                        changed=changed,
                        error=outcome_error,
                    )
                )
            else:
                message = inspection.error or "invalid canonical CSV"
                current = self.get_date_state(date_text)
                changed = self._set_artifact_state(
                    date_text,
                    PersistentSyncStatus.FILE_CORRUPT,
                    checksum=inspection.checksum,
                    path=inspection.path,
                    error_type="FILE_CORRUPT",
                    error_message=message,
                    preserve_artifact=(
                        current is not None
                        and current.last_verified_at is not None
                        and current.csv_checksum_sha256 is not None
                    ),
                )
                outcomes.append(
                    BootstrapFileResult(
                        path=path,
                        market_date=date_text,
                        status=PersistentSyncStatus.FILE_CORRUPT,
                        row_count=inspection.row_count,
                        checksum=inspection.checksum,
                        changed=changed,
                        error=message,
                    )
                )

        indexed = sum(
            item.changed and item.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
            for item in outcomes
        )
        invalid = sum(
            item.status
            in {
                PersistentSyncStatus.FILE_CORRUPT,
                PersistentSyncStatus.FILE_CONFLICT,
            }
            for item in outcomes
        )
        unchanged = sum(
            not item.changed
            and item.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
            for item in outcomes
        )
        return BootstrapResult(
            discovered_files=len(outcomes),
            indexed_files=indexed,
            unchanged_files=unchanged,
            invalid_files=invalid,
            files=tuple(outcomes),
        )

    def finish_sync_run(
        self,
        run_id: str,
        *,
        interrupted: bool = False,
        duration_ms: float | None = None,
    ) -> SyncRunRecord:
        finished_at = utc_now_iso()
        with self._connect() as connection:
            run = connection.execute(
                "SELECT * FROM sync_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                raise StateDatabaseError(f"unknown sync run: {run_id}")
            results = connection.execute(
                "SELECT * FROM sync_run_date_results WHERE run_id = ?", (run_id,)
            ).fetchall()
            attempts = connection.execute(
                """
                SELECT COUNT(*) AS total_attempts,
                       COALESCE(SUM(response_bytes), 0) AS total_response_bytes,
                       COUNT(DISTINCT market_date) AS network_fetch_count
                FROM download_attempts WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            status_counts = Counter(row["status"] for row in results)
            success_count = sum(
                status_counts.get(status.value, 0)
                for status in (
                    DownloadStatus.TRADING_DATA,
                    DownloadStatus.ALREADY_PRESENT,
                )
            )
            unresolved_count = sum(
                status_counts.get(status.value, 0)
                for status in (
                    DownloadStatus.EMPTY_MARKET_RESPONSE,
                    DownloadStatus.NON_TRADING_OR_EMPTY,
                )
            )
            failure_count = len(results) - success_count - unresolved_count
            if interrupted:
                run_status = SyncRunStatus.INTERRUPTED
            elif failure_count:
                run_status = SyncRunStatus.COMPLETED_WITH_FAILURES
            elif unresolved_count:
                run_status = SyncRunStatus.COMPLETED_WITH_UNRESOLVED
            else:
                run_status = SyncRunStatus.COMPLETED
            if duration_ms is None:
                started = datetime.fromisoformat(run["started_at"])
                duration_ms = (
                    datetime.fromisoformat(finished_at) - started
                ).total_seconds() * 1000
            connection.execute(
                """
                UPDATE sync_runs SET
                    finished_at = ?, duration_ms = ?, completed_count = ?,
                    network_fetch_count = ?, local_skip_count = ?,
                    success_count = ?, unresolved_count = ?, failure_count = ?,
                    total_valid_rows = ?, total_rejected_rows = ?,
                    total_response_bytes = ?, total_attempts = ?,
                    interrupted = ?, status = ?
                WHERE run_id = ?
                """,
                (
                    finished_at,
                    duration_ms,
                    len(results),
                    attempts["network_fetch_count"],
                    sum(row["local_skip"] for row in results),
                    success_count,
                    unresolved_count,
                    failure_count,
                    sum(row["valid_row_count"] for row in results),
                    sum(row["rejected_row_count"] for row in results),
                    attempts["total_response_bytes"],
                    attempts["total_attempts"],
                    int(interrupted),
                    run_status.value,
                    run_id,
                ),
            )
        record = self.get_sync_run(run_id)
        assert record is not None
        return record

    @staticmethod
    def _run_from_row(row: sqlite3.Row) -> SyncRunRecord:
        return SyncRunRecord(
            run_id=row["run_id"],
            command_type=row["command_type"],
            start_date=row["start_date"],
            end_date=row["end_date"],
            requested_date_count=row["requested_date_count"],
            worker_count=row["worker_count"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            duration_ms=row["duration_ms"],
            completed_count=row["completed_count"],
            network_fetch_count=row["network_fetch_count"],
            local_skip_count=row["local_skip_count"],
            success_count=row["success_count"],
            unresolved_count=row["unresolved_count"],
            failure_count=row["failure_count"],
            total_valid_rows=row["total_valid_rows"],
            total_rejected_rows=row["total_rejected_rows"],
            total_response_bytes=row["total_response_bytes"],
            total_attempts=row["total_attempts"],
            interrupted=bool(row["interrupted"]),
            status=SyncRunStatus(row["status"]),
            application_version=row["application_version"],
        )

    def get_sync_run(self, run_id: str) -> SyncRunRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM sync_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return None if row is None else self._run_from_row(row)

    def get_recent_attempts(
        self, market_date: str, *, limit: int = 5
    ) -> tuple[DownloadAttemptRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM download_attempts
                WHERE market_date = ? ORDER BY id DESC LIMIT ?
                """,
                (market_date, limit),
            ).fetchall()
        return tuple(
            DownloadAttemptRecord(
                id=row["id"],
                run_id=row["run_id"],
                market_date=row["market_date"],
                attempt_number=row["attempt_number"],
                started_at=row["started_at"],
                finished_at=row["finished_at"],
                duration_ms=row["duration_ms"],
                http_status=row["http_status"],
                response_bytes=row["response_bytes"],
                response_classification=row["response_classification"],
                final_status=row["final_status"],
                retryable=bool(row["retryable"]),
                error_type=row["error_type"],
                error_message=row["error_message"],
                parsed_row_count=row["parsed_row_count"],
                valid_row_count=row["valid_row_count"],
                rejected_row_count=row["rejected_row_count"],
                checksum=row["checksum"],
                csv_relative_path=row["csv_relative_path"],
                worker_identifier=row["worker_identifier"],
                created_at=row["created_at"],
            )
            for row in rows
        )

    def list_dates_by_status(
        self,
        statuses: Iterable[PersistentSyncStatus],
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> tuple[DateSyncState, ...]:
        values = tuple(status.value for status in statuses)
        if not values:
            return ()
        clauses = [f"status IN ({','.join('?' for _ in values)})"]
        parameters: list[object] = list(values)
        if start_date is not None:
            clauses.append("market_date >= ?")
            parameters.append(start_date)
        if end_date is not None:
            clauses.append("market_date <= ?")
            parameters.append(end_date)
        query = (
            "SELECT * FROM date_sync_state WHERE "
            + " AND ".join(clauses)
            + " ORDER BY market_date"
        )
        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return tuple(self._state_from_row(row) for row in rows)

    def summarize_range(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> StateSummary:
        clauses: list[str] = []
        parameters: list[object] = []
        if start_date is not None:
            clauses.append("market_date >= ?")
            parameters.append(start_date)
        if end_date is not None:
            clauses.append("market_date <= ?")
            parameters.append(end_date)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._connect() as connection:
            counts = connection.execute(
                "SELECT status, COUNT(*) AS count FROM date_sync_state"
                + where
                + " GROUP BY status",
                parameters,
            ).fetchall()
            bounds = connection.execute(
                "SELECT COUNT(*) AS tracked, MIN(market_date) AS earliest, "
                "MAX(market_date) AS latest, MAX(last_success_at) AS last_success "
                "FROM date_sync_state" + where,
                parameters,
            ).fetchone()
        counter = {status: 0 for status in PersistentSyncStatus}
        for row in counts:
            counter[PersistentSyncStatus(row["status"])] = row["count"]
        return StateSummary(
            database_path=self.database_path,
            tracked_dates=bounds["tracked"],
            counts_by_status=counter,
            earliest_tracked=bounds["earliest"],
            latest_tracked=bounds["latest"],
            last_successful_sync=bounds["last_success"],
        )


class AsyncStateRepository:
    """Async façade with one serialized writer and thread-local connections."""

    def __init__(self, repository: StateRepository) -> None:
        self.repository = repository
        self._write_lock = asyncio.Lock()

    async def prepare_fetch(
        self,
        market_date: date,
        output_dir: Path,
        columns: tuple[str, ...] = CANONICAL_COLUMNS,
    ) -> DownloadResult | None:
        async with self._write_lock:
            return await asyncio.to_thread(
                self.repository.prepare_fetch,
                market_date,
                output_dir,
                columns,
            )

    async def record_attempt(
        self, run_id: str, event: DownloadAttemptEvent
    ) -> None:
        async with self._write_lock:
            await asyncio.to_thread(self.repository.record_attempt, run_id, event)

    async def record_download_result(
        self, run_id: str, result: DownloadResult
    ) -> None:
        async with self._write_lock:
            await asyncio.to_thread(
                self.repository.record_download_result, run_id, result
            )
