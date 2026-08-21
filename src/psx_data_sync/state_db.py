"""Versioned SQLite persistence for synchronization metadata.

The repository opens a short-lived connection per operation. Async callers use
``AsyncStateRepository`` to move those small operations off the event loop and
serialize writes with one lock; network work remains fully concurrent.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import time
import uuid
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from . import __version__
from .config import CANONICAL_COLUMNS, Settings
from .exporter import (
    StagedPromotionStatus,
    inspect_canonical_csv_file,
    inspect_existing_canonical_file,
    promote_staged_csv_if_safe,
)
from .state import (
    BootstrapFileResult,
    BootstrapResult,
    AttemptEvidenceRecord,
    DashboardSummary,
    DateSyncState,
    DateReconciliationResult,
    DownloadAttemptEvent,
    DownloadAttemptRecord,
    DownloadResult,
    DownloadStatus,
    ExistingFileInspection,
    FileHealthState,
    LogActivityItem,
    ParquetExportRecord,
    ParquetExportStatus,
    PersistentSyncStatus,
    StateSummary,
    SyncEvidenceState,
    ReconciliationMode,
    ReconciliationRangeResult,
    ReconciliationRunRecord,
    ReconciliationRunStatus,
    RECONCILIATION_POLICY_VERSION,
    SyncRunRecord,
    SyncRunStatus,
    WEEKEND_EMPTY_CLASSIFICATION_BASIS,
)


SCHEMA_VERSION = 3
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
ARTIFACT_ISSUE_STATUSES = frozenset(
    {
        PersistentSyncStatus.FILE_MISSING,
        PersistentSyncStatus.FILE_CORRUPT,
        PersistentSyncStatus.FILE_CONFLICT,
    }
)
LOCAL_ARTIFACT_DOWNLOAD_STATUSES = frozenset(
    {
        DownloadStatus.REPAIR_REQUIRED,
        DownloadStatus.EXISTING_FILE_INVALID,
        DownloadStatus.FILE_CONFLICT,
    }
)
EXPECTED_TABLES = frozenset(
    {
        "sync_schema_metadata",
        "date_sync_state",
        "sync_runs",
        "download_attempts",
        "sync_run_date_results",
        "reconciliation_runs",
        "reconciliation_events",
        "repair_candidates",
        "reconciliation_recheck_claims",
        "parquet_exports",
    }
)
EXPECTED_INDEXES = frozenset(
    {
        "idx_date_sync_state_status",
        "idx_download_attempts_market_date",
        "idx_download_attempts_run_id",
        "idx_sync_run_date_results_market_date",
        "idx_sync_runs_started_at",
        "idx_reconciliation_runs_started_at",
        "idx_reconciliation_events_market_date",
        "idx_reconciliation_events_run_id",
        "idx_reconciliation_events_unique_decision",
        "idx_reconciliation_runs_linked_sync_run",
        "idx_download_attempts_evidence",
        "idx_repair_candidates_market_date",
        "idx_recheck_claims_expires_at",
        "idx_parquet_exports_status",
    }
)
EXPECTED_DATE_STATE_COLUMNS = frozenset(
    {
        "classification_policy_version",
        "classification_basis",
        "classification_updated_at",
        "next_recheck_after",
        "recheck_policy_version",
    }
)
EXPECTED_RECONCILIATION_COLUMNS = {
    "reconciliation_runs": frozenset(
        {
            "run_id",
            "policy_version",
            "start_date",
            "end_date",
            "mode",
            "requested_date_count",
            "worker_count",
            "force_recheck",
            "max_rechecks_per_date",
            "cooldown_seconds",
            "verified_count",
            "confirmed_non_trading_count",
            "never_attempted_count",
            "unresolved_count",
            "failure_count",
            "file_health_issue_count",
            "network_recheck_planned_count",
            "network_recheck_count",
            "local_repair_count",
            "manual_review_count",
            "status_transition_count",
            "complete",
            "linked_sync_run_id",
            "started_at",
            "finished_at",
            "duration_ms",
            "interrupted",
            "status",
            "error_message",
            "application_version",
        }
    ),
    "reconciliation_events": frozenset(
        {
            "id",
            "run_id",
            "market_date",
            "previous_status",
            "new_status",
            "action",
            "policy_version",
            "evidence_classification",
            "evidence_summary",
            "created_at",
        }
    ),
    "repair_candidates": frozenset(
        {
            "id",
            "reconciliation_run_id",
            "market_date",
            "staged_relative_path",
            "prior_checksum_sha256",
            "candidate_checksum_sha256",
            "prior_row_count",
            "candidate_row_count",
            "validation_state",
            "disposition",
            "created_at",
            "evaluated_at",
            "promoted_at",
            "message",
        }
    ),
    "reconciliation_recheck_claims": frozenset(
        {"market_date", "reconciliation_run_id", "claimed_at", "expires_at"}
    ),
}
EXPECTED_PARQUET_COLUMNS = {
    "parquet_exports": frozenset(
        {
            "market_date",
            "status",
            "schema_version",
            "source_csv_checksum_sha256",
            "source_row_count",
            "parquet_relative_path",
            "parquet_checksum_sha256",
            "parquet_row_count",
            "exporter_version",
            "created_at",
            "updated_at",
            "verified_at",
            "last_error",
        }
    ),
}
EXPECTED_TABLE_COLUMNS = {
    "sync_schema_metadata": frozenset(
        {
            "singleton",
            "schema_version",
            "created_at",
            "updated_at",
            "application_version",
        }
    ),
    "date_sync_state": frozenset(
        {
            "market_date",
            "status",
            "evidence_state",
            "attempt_count",
            "successful_attempt_count",
            "last_http_status",
            "last_response_bytes",
            "parsed_row_count",
            "valid_row_count",
            "rejected_row_count",
            "csv_checksum_sha256",
            "csv_relative_path",
            "first_attempt_at",
            "last_attempt_at",
            "last_success_at",
            "last_verified_at",
            "last_error_type",
            "last_error_message",
            "last_duration_ms",
            "source_endpoint",
            "record_created_at",
            "record_updated_at",
            *EXPECTED_DATE_STATE_COLUMNS,
        }
    ),
    "sync_runs": frozenset(
        {
            "run_id",
            "command_type",
            "start_date",
            "end_date",
            "requested_date_count",
            "worker_count",
            "started_at",
            "finished_at",
            "duration_ms",
            "completed_count",
            "network_fetch_count",
            "local_skip_count",
            "success_count",
            "unresolved_count",
            "failure_count",
            "total_valid_rows",
            "total_rejected_rows",
            "total_response_bytes",
            "total_attempts",
            "interrupted",
            "status",
            "application_version",
        }
    ),
    "download_attempts": frozenset(
        {
            "id",
            "run_id",
            "market_date",
            "attempt_number",
            "started_at",
            "finished_at",
            "duration_ms",
            "http_status",
            "response_bytes",
            "response_classification",
            "final_status",
            "retryable",
            "error_type",
            "error_message",
            "parsed_row_count",
            "valid_row_count",
            "rejected_row_count",
            "checksum",
            "csv_relative_path",
            "worker_identifier",
            "created_at",
        }
    ),
    "sync_run_date_results": frozenset(
        {
            "run_id",
            "market_date",
            "status",
            "attempts_in_run",
            "local_skip",
            "parsed_row_count",
            "valid_row_count",
            "rejected_row_count",
            "response_bytes",
            "checksum",
            "csv_relative_path",
            "duration_ms",
            "error_message",
            "created_at",
        }
    ),
    **EXPECTED_RECONCILIATION_COLUMNS,
    **EXPECTED_PARQUET_COLUMNS,
}
EXPECTED_PRIMARY_KEYS = {
    "sync_schema_metadata": ("singleton",),
    "date_sync_state": ("market_date",),
    "sync_runs": ("run_id",),
    "download_attempts": ("id",),
    "sync_run_date_results": ("run_id", "market_date"),
    "reconciliation_runs": ("run_id",),
    "reconciliation_events": ("id",),
    "repair_candidates": ("id",),
    "reconciliation_recheck_claims": ("market_date",),
    "parquet_exports": ("market_date",),
}
EXPECTED_UNIQUE_COLUMN_SETS = {
    "download_attempts": frozenset(
        {("run_id", "market_date", "attempt_number")}
    ),
    "repair_candidates": frozenset(
        {("reconciliation_run_id", "market_date")}
    ),
}
EXPECTED_FOREIGN_KEYS = {
    "download_attempts": frozenset(
        {
            ("sync_runs", "run_id", "run_id", "NO ACTION", "NO ACTION"),
            (
                "date_sync_state",
                "market_date",
                "market_date",
                "NO ACTION",
                "NO ACTION",
            ),
        }
    ),
    "sync_run_date_results": frozenset(
        {
            ("sync_runs", "run_id", "run_id", "NO ACTION", "NO ACTION"),
            (
                "date_sync_state",
                "market_date",
                "market_date",
                "NO ACTION",
                "NO ACTION",
            ),
        }
    ),
    "reconciliation_runs": frozenset(
        {
            (
                "sync_runs",
                "linked_sync_run_id",
                "run_id",
                "NO ACTION",
                "NO ACTION",
            )
        }
    ),
    "reconciliation_events": frozenset(
        {
            (
                "reconciliation_runs",
                "run_id",
                "run_id",
                "NO ACTION",
                "NO ACTION",
            )
        }
    ),
    "repair_candidates": frozenset(
        {
            (
                "reconciliation_runs",
                "reconciliation_run_id",
                "run_id",
                "NO ACTION",
                "NO ACTION",
            )
        }
    ),
    "reconciliation_recheck_claims": frozenset(
        {
            (
                "reconciliation_runs",
                "reconciliation_run_id",
                "run_id",
                "NO ACTION",
                "NO ACTION",
            )
        }
    ),
    "parquet_exports": frozenset(
        {
            (
                "date_sync_state",
                "market_date",
                "market_date",
                "NO ACTION",
                "NO ACTION",
            )
        }
    ),
}
# name -> (table, columns, unique, partial predicate)
EXPECTED_INDEX_SHAPES = {
    "idx_date_sync_state_status": (
        "date_sync_state",
        ("status",),
        False,
        None,
    ),
    "idx_download_attempts_market_date": (
        "download_attempts",
        ("market_date", "created_at"),
        False,
        None,
    ),
    "idx_download_attempts_run_id": (
        "download_attempts",
        ("run_id",),
        False,
        None,
    ),
    "idx_sync_run_date_results_market_date": (
        "sync_run_date_results",
        ("market_date",),
        False,
        None,
    ),
    "idx_sync_runs_started_at": ("sync_runs", ("started_at",), False, None),
    "idx_reconciliation_runs_started_at": (
        "reconciliation_runs",
        ("started_at",),
        False,
        None,
    ),
    "idx_reconciliation_events_market_date": (
        "reconciliation_events",
        ("market_date", "created_at"),
        False,
        None,
    ),
    "idx_reconciliation_events_run_id": (
        "reconciliation_events",
        ("run_id",),
        False,
        None,
    ),
    "idx_reconciliation_events_unique_decision": (
        "reconciliation_events",
        ("run_id", "market_date", "action", "new_status"),
        True,
        None,
    ),
    "idx_reconciliation_runs_linked_sync_run": (
        "reconciliation_runs",
        ("linked_sync_run_id",),
        True,
        "where linked_sync_run_id is not null",
    ),
    "idx_download_attempts_evidence": (
        "download_attempts",
        (
            "market_date",
            "response_classification",
            "final_status",
            "finished_at",
        ),
        False,
        None,
    ),
    "idx_repair_candidates_market_date": (
        "repair_candidates",
        ("market_date", "created_at"),
        False,
        None,
    ),
    "idx_recheck_claims_expires_at": (
        "reconciliation_recheck_claims",
        ("expires_at",),
        False,
        None,
    ),
    "idx_parquet_exports_status": (
        "parquet_exports",
        ("status",),
        False,
        None,
    ),
}


class StateDatabaseError(RuntimeError):
    """Base persistence-layer error."""


class IncompatibleSchemaError(StateDatabaseError):
    """Raised instead of silently changing an unknown schema version."""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def persistent_status_for_download(status: DownloadStatus) -> PersistentSyncStatus:
    if status is DownloadStatus.REPAIR_REQUIRED:
        raise ValueError(
            "REPAIR_REQUIRED is a derived action and cannot replace date state"
        )
    mapping = {
        DownloadStatus.TRADING_DATA: PersistentSyncStatus.VERIFIED_TRADING_DATA,
        DownloadStatus.ALREADY_PRESENT: PersistentSyncStatus.VERIFIED_TRADING_DATA,
        DownloadStatus.EMPTY_MARKET_RESPONSE: PersistentSyncStatus.EMPTY_UNRESOLVED,
        DownloadStatus.NON_TRADING_OR_EMPTY: PersistentSyncStatus.EMPTY_UNRESOLVED,
        DownloadStatus.CONFIRMED_NON_TRADING: (
            PersistentSyncStatus.CONFIRMED_NON_TRADING
        ),
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


def _has_trusted_artifact_identity(state: DateSyncState | None) -> bool:
    return (
        state is not None
        and state.last_verified_at is not None
        and state.csv_checksum_sha256 is not None
        and state.valid_row_count > 0
    )


def _matches_trusted_artifact_identity(
    state: DateSyncState,
    *,
    checksum: str | None,
    row_count: int,
) -> bool:
    return (
        _has_trusted_artifact_identity(state)
        and checksum == state.csv_checksum_sha256
        and row_count == state.valid_row_count
    )


def resolve_status_transition(
    previous: PersistentSyncStatus,
    incoming: PersistentSyncStatus,
) -> PersistentSyncStatus:
    """Apply the only permitted current-state transition policy."""

    if incoming is PersistentSyncStatus.VERIFIED_TRADING_DATA:
        return incoming
    if incoming is PersistentSyncStatus.CONFIRMED_NON_TRADING:
        return previous if previous in VERIFIED_STATUSES else incoming
    if incoming is PersistentSyncStatus.ALREADY_PRESENT_VERIFIED:
        return (
            previous
            if previous in VERIFIED_STATUSES
            else PersistentSyncStatus.VERIFIED_TRADING_DATA
        )
    if (
        previous is PersistentSyncStatus.CONFIRMED_NON_TRADING
        and incoming in TRANSIENT_STATUSES
    ):
        return previous
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


def _validate_artifact_snapshot(
    inspection: ExistingFileInspection,
    *,
    market_date: str,
    expected_artifact_path: Path | None,
    expected_artifact_exists: bool | None,
    expected_artifact_valid: bool | None,
    expected_observed_checksum: str | None,
) -> None:
    """Reject a stale artifact plan before any preflight branch is selected."""

    if expected_artifact_path is None:
        if any(
            value is not None
            for value in (
                expected_artifact_exists,
                expected_artifact_valid,
                expected_observed_checksum,
            )
        ):
            raise StateDatabaseError(
                "artifact snapshot values require an expected artifact path"
            )
        return
    if Path(expected_artifact_path).resolve() != inspection.path.resolve():
        raise StateDatabaseError(
            f"artifact path changed after reconciliation planning for {market_date}"
        )
    if (
        expected_artifact_exists is not None
        and inspection.exists != expected_artifact_exists
    ):
        raise StateDatabaseError(
            f"artifact changed after reconciliation planning for {market_date}"
        )
    if (
        expected_artifact_valid is not None
        and inspection.valid != expected_artifact_valid
    ):
        raise StateDatabaseError(
            f"artifact validity changed after planning for {market_date}"
        )
    if (
        expected_observed_checksum is not None
        and inspection.checksum != expected_observed_checksum
    ):
        raise StateDatabaseError(
            f"artifact checksum changed after planning for {market_date}"
        )


def _serialized_reconciliation_evidence(
    result: DateReconciliationResult,
) -> str:
    evidence: dict[str, Any] = asdict(result.evidence_summary)
    # Attempts are retained losslessly in ``download_attempts``. Decision
    # events deliberately keep a compact aggregate plus a bounded recent tail
    # so a long-lived date can always be reconciled without hitting the event
    # size guard.
    history_limit = 50

    http_statuses = list(result.evidence_summary.http_statuses)
    status_counts: dict[str, int] = {}
    for s in http_statuses:
        label = str(s)
        status_counts[label] = status_counts.get(label, 0) + 1
    evidence["http_status_counts"] = dict(sorted(status_counts.items()))
    evidence["http_statuses_total_count"] = len(http_statuses)
    evidence["http_statuses_truncated_count"] = max(
        len(http_statuses) - history_limit, 0
    )
    evidence["http_statuses"] = http_statuses[-history_limit:]

    response_classifications = list(
        result.evidence_summary.response_classifications
    )
    class_counts: dict[str, int] = {}
    for c in response_classifications:
        class_counts[c] = class_counts.get(c, 0) + 1
    evidence["response_classification_counts"] = dict(sorted(class_counts.items()))
    evidence["response_classifications_total_count"] = len(
        response_classifications
    )
    evidence["response_classifications_truncated_count"] = max(
        len(response_classifications) - history_limit, 0
    )
    evidence["response_classifications"] = response_classifications[
        -history_limit:
    ]

    evidence["reasons"] = result.reasons
    evidence["warnings"] = result.warnings
    serialized = json.dumps(evidence, sort_keys=True, separators=(",", ":"))
    if len(serialized) > 8_000:
        raise StateDatabaseError("reconciliation evidence summary is too large")
    return serialized


def _insert_reconciliation_event(
    connection: sqlite3.Connection,
    run_id: str,
    result: DateReconciliationResult,
) -> bool:
    cursor = connection.execute(
        """
        INSERT INTO reconciliation_events (
            run_id, market_date, previous_status, new_status, action,
            policy_version, evidence_classification,
            evidence_summary, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, market_date, action, new_status) DO NOTHING
        """,
        (
            run_id,
            result.market_date,
            result.previous_status.value,
            result.reconciled_status.value,
            result.action_required.value,
            result.policy_version,
            result.evidence_classification,
            _serialized_reconciliation_evidence(result),
            utc_now_iso(),
        ),
    )
    return cursor.rowcount == 1


def _validate_reconciliation_mutation_event(
    connection: sqlite3.Connection,
    run_id: str,
    result: DateReconciliationResult,
    *,
    market_date: str,
    new_status: PersistentSyncStatus,
) -> None:
    """Ensure an atomic mutation cannot be paired with a stale/misleading event."""

    if result.market_date != market_date:
        raise StateDatabaseError(
            "reconciliation event date does not match the state mutation"
        )
    if result.reconciled_status is not new_status:
        raise StateDatabaseError(
            "reconciliation event status does not match the state mutation"
        )
    run = connection.execute(
        """
        SELECT policy_version, start_date, end_date, mode, status
        FROM reconciliation_runs WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    if run is None:
        raise StateDatabaseError(f"unknown reconciliation run: {run_id}")
    if result.policy_version != run["policy_version"]:
        raise StateDatabaseError(
            "reconciliation event policy does not match its run"
        )
    if not (run["start_date"] <= market_date <= run["end_date"]):
        raise StateDatabaseError(
            "reconciliation event date is outside its run range"
        )
    if (
        run["mode"] != ReconciliationMode.APPLY.value
        or run["status"] != ReconciliationRunStatus.RUNNING.value
    ):
        raise StateDatabaseError(
            "state-changing reconciliation events require a running apply run"
        )


class StateRepository:
    """Encapsulate all SQL and state transitions for one SQLite database."""

    def __init__(
        self,
        database_path: Path,
        *,
        project_root: Path | None = None,
        raw_output_dir: Path | None = None,
        source_endpoint: str = Settings().historical_url,
        application_version: str = __version__,
    ) -> None:
        self.database_path = Path(database_path)
        self.project_root = (project_root or Path.cwd()).resolve()
        if raw_output_dir is not None:
            self.raw_output_dir = Path(raw_output_dir).resolve()
        elif "PSX_RAW_OUTPUT_DIR" in os.environ:
            self.raw_output_dir = (
                Path(os.environ["PSX_RAW_OUTPUT_DIR"]).expanduser().resolve()
            )
        elif project_root is not None:
            self.raw_output_dir = (self.project_root / "data" / "raw").resolve()
        else:
            self.raw_output_dir = Settings.from_env().raw_output_dir.resolve()
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
        connection = self._connect()
        try:
            metadata_table = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'sync_schema_metadata'
                """
            ).fetchone()
            if metadata_table is not None:
                preflight = connection.execute(
                    """
                    SELECT schema_version FROM sync_schema_metadata
                    WHERE singleton = 1
                    """
                ).fetchone()
                if preflight is not None and preflight["schema_version"] not in {
                    1,
                    2,
                    SCHEMA_VERSION,
                }:
                    raise IncompatibleSchemaError(
                        "unsupported sync schema version "
                        f"{preflight['schema_version']}; expected {SCHEMA_VERSION}"
                    )
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("BEGIN IMMEDIATE")
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
            if existing is not None and existing["schema_version"] not in {
                1,
                2,
                SCHEMA_VERSION,
            }:
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
                    classification_policy_version TEXT,
                    classification_basis TEXT,
                    classification_updated_at TEXT,
                    next_recheck_after TEXT,
                    recheck_policy_version TEXT,
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
            date_state_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(date_sync_state)")
            }
            for column_name, declaration in (
                ("classification_policy_version", "TEXT"),
                ("classification_basis", "TEXT"),
                ("classification_updated_at", "TEXT"),
                ("next_recheck_after", "TEXT"),
                ("recheck_policy_version", "TEXT"),
            ):
                if column_name not in date_state_columns:
                    connection.execute(
                        f"ALTER TABLE date_sync_state ADD COLUMN "
                        f"{column_name} {declaration}"
                    )
            connection.execute(
                """
                UPDATE date_sync_state
                SET status = ?
                WHERE status = ?
                """,
                (
                    PersistentSyncStatus.VERIFIED_TRADING_DATA.value,
                    PersistentSyncStatus.ALREADY_PRESENT_VERIFIED.value,
                ),
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS reconciliation_runs (
                    run_id TEXT PRIMARY KEY,
                    policy_version TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    requested_date_count INTEGER NOT NULL,
                    worker_count INTEGER NOT NULL,
                    force_recheck INTEGER NOT NULL DEFAULT 0,
                    max_rechecks_per_date INTEGER NOT NULL DEFAULT 1,
                    cooldown_seconds REAL NOT NULL DEFAULT 86400,
                    verified_count INTEGER NOT NULL DEFAULT 0,
                    confirmed_non_trading_count INTEGER NOT NULL DEFAULT 0,
                    never_attempted_count INTEGER NOT NULL DEFAULT 0,
                    unresolved_count INTEGER NOT NULL DEFAULT 0,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    file_health_issue_count INTEGER NOT NULL DEFAULT 0,
                    network_recheck_planned_count INTEGER NOT NULL DEFAULT 0,
                    network_recheck_count INTEGER NOT NULL DEFAULT 0,
                    local_repair_count INTEGER NOT NULL DEFAULT 0,
                    manual_review_count INTEGER NOT NULL DEFAULT 0,
                    status_transition_count INTEGER NOT NULL DEFAULT 0,
                    complete INTEGER NOT NULL DEFAULT 0,
                    linked_sync_run_id TEXT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    duration_ms REAL,
                    interrupted INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    application_version TEXT NOT NULL,
                    FOREIGN KEY (linked_sync_run_id) REFERENCES sync_runs(run_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS reconciliation_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    market_date TEXT NOT NULL,
                    previous_status TEXT NOT NULL,
                    new_status TEXT NOT NULL,
                    action TEXT NOT NULL,
                    policy_version TEXT NOT NULL,
                    evidence_classification TEXT NOT NULL,
                    evidence_summary TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES reconciliation_runs(run_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS repair_candidates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    reconciliation_run_id TEXT NOT NULL,
                    market_date TEXT NOT NULL,
                    staged_relative_path TEXT NOT NULL,
                    prior_checksum_sha256 TEXT,
                    candidate_checksum_sha256 TEXT,
                    prior_row_count INTEGER,
                    candidate_row_count INTEGER,
                    validation_state TEXT NOT NULL,
                    disposition TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    evaluated_at TEXT,
                    promoted_at TEXT,
                    message TEXT,
                    UNIQUE (reconciliation_run_id, market_date),
                    FOREIGN KEY (reconciliation_run_id)
                        REFERENCES reconciliation_runs(run_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS reconciliation_recheck_claims (
                    market_date TEXT PRIMARY KEY,
                    reconciliation_run_id TEXT NOT NULL,
                    claimed_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    FOREIGN KEY (reconciliation_run_id)
                        REFERENCES reconciliation_runs(run_id)
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_reconciliation_runs_started_at "
                "ON reconciliation_runs(started_at)"
            )
            reconciliation_run_columns = {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(reconciliation_runs)"
                )
            }
            if "cooldown_seconds" not in reconciliation_run_columns:
                connection.execute(
                    "ALTER TABLE reconciliation_runs ADD COLUMN "
                    "cooldown_seconds REAL NOT NULL DEFAULT 86400"
                )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_reconciliation_events_market_date "
                "ON reconciliation_events(market_date, created_at)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_reconciliation_events_run_id "
                "ON reconciliation_events(run_id)"
            )
            connection.execute(
                "DROP INDEX IF EXISTS idx_reconciliation_events_unique_decision"
            )
            connection.execute(
                "CREATE UNIQUE INDEX idx_reconciliation_events_unique_decision "
                "ON reconciliation_events(run_id, market_date, action, new_status)"
            )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                "idx_reconciliation_runs_linked_sync_run "
                "ON reconciliation_runs(linked_sync_run_id) "
                "WHERE linked_sync_run_id IS NOT NULL"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_download_attempts_evidence "
                "ON download_attempts(market_date, response_classification, "
                "final_status, finished_at)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_repair_candidates_market_date "
                "ON repair_candidates(market_date, created_at)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_recheck_claims_expires_at "
                "ON reconciliation_recheck_claims(expires_at)"
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS parquet_exports (
                    market_date TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    source_csv_checksum_sha256 TEXT NOT NULL,
                    source_row_count INTEGER NOT NULL CHECK (source_row_count >= 0),
                    parquet_relative_path TEXT,
                    parquet_checksum_sha256 TEXT,
                    parquet_row_count INTEGER CHECK (parquet_row_count IS NULL OR parquet_row_count >= 0),
                    exporter_version TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    verified_at TEXT,
                    last_error TEXT,
                    FOREIGN KEY (market_date) REFERENCES date_sync_state(market_date)
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_parquet_exports_status "
                "ON parquet_exports(status)"
            )
            connection.execute(
                """
                UPDATE sync_schema_metadata
                SET schema_version = ?, updated_at = ?, application_version = ?
                WHERE singleton = 1
                """,
                (SCHEMA_VERSION, now, self.application_version),
            )
            # Verify the complete target shape before commit.  SQLite DDL is
            # transactional here, so a malformed purported v1/v2 database
            # cannot be stamped as v2 after only a partial migration.
            self._verify_schema_connection(connection)
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

        # Re-open once after commit as an end-to-end guard that initialization
        # never returns successfully for a schema consumers cannot use.
        self.verify_schema()

    @staticmethod
    def _verify_schema_connection(connection: sqlite3.Connection) -> int:
        """Verify the complete v2 shape using the caller's transaction."""

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

        table_info: dict[str, tuple[sqlite3.Row, ...]] = {}
        for table, expected_columns in EXPECTED_TABLE_COLUMNS.items():
            rows = tuple(connection.execute(f"PRAGMA table_info({table})"))
            table_info[table] = rows
            observed_columns = {row["name"] for row in rows}
            missing = expected_columns - observed_columns
            if missing:
                raise StateDatabaseError(
                    f"state database is missing {table} columns: "
                    + ", ".join(sorted(missing))
                )

        for table, expected_primary_key in EXPECTED_PRIMARY_KEYS.items():
            observed_primary_key = tuple(
                row["name"]
                for row in sorted(table_info[table], key=lambda item: item["pk"])
                if row["pk"]
            )
            if observed_primary_key != expected_primary_key:
                raise StateDatabaseError(
                    f"state database has incompatible {table} primary key: "
                    f"expected {expected_primary_key!r}, "
                    f"found {observed_primary_key!r}"
                )

        for table, expected_unique_shapes in EXPECTED_UNIQUE_COLUMN_SETS.items():
            observed_unique_shapes = {
                tuple(
                    row["name"]
                    for row in connection.execute(
                        f"PRAGMA index_info({index_row['name']})"
                    )
                )
                for index_row in connection.execute(f"PRAGMA index_list({table})")
                if index_row["unique"]
            }
            missing_unique_shapes = (
                expected_unique_shapes - observed_unique_shapes
            )
            if missing_unique_shapes:
                raise StateDatabaseError(
                    f"state database is missing {table} unique constraints: "
                    + ", ".join(
                        repr(shape) for shape in sorted(missing_unique_shapes)
                    )
                )

        for table, expected_foreign_keys in EXPECTED_FOREIGN_KEYS.items():
            observed_foreign_keys = {
                (
                    row["table"],
                    row["from"],
                    row["to"],
                    row["on_update"],
                    row["on_delete"],
                )
                for row in connection.execute(f"PRAGMA foreign_key_list({table})")
            }
            missing_foreign_keys = expected_foreign_keys - observed_foreign_keys
            if missing_foreign_keys:
                raise StateDatabaseError(
                    f"state database is missing {table} foreign keys: "
                    + ", ".join(
                        repr(shape) for shape in sorted(missing_foreign_keys)
                    )
                )

        for index_name, expected_shape in EXPECTED_INDEX_SHAPES.items():
            expected_table, expected_columns, expected_unique, predicate = (
                expected_shape
            )
            schema_row = connection.execute(
                """
                SELECT tbl_name, sql FROM sqlite_master
                WHERE type = 'index' AND name = ?
                """,
                (index_name,),
            ).fetchone()
            assert schema_row is not None
            index_list_row = next(
                (
                    row
                    for row in connection.execute(
                        f"PRAGMA index_list({schema_row['tbl_name']})"
                    )
                    if row["name"] == index_name
                ),
                None,
            )
            observed_columns = tuple(
                row["name"]
                for row in connection.execute(f"PRAGMA index_info({index_name})")
            )
            normalized_sql = " ".join(
                (schema_row["sql"] or "").lower().split()
            )
            expected_partial = predicate is not None
            if (
                schema_row["tbl_name"] != expected_table
                or index_list_row is None
                or observed_columns != expected_columns
                or bool(index_list_row["unique"]) != expected_unique
                or bool(index_list_row["partial"]) != expected_partial
                or (predicate is not None and predicate not in normalized_sql)
            ):
                raise StateDatabaseError(
                    f"state database index {index_name} has an incompatible shape"
                )

        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise StateDatabaseError(
                "state database failed foreign-key validation"
            )
        return metadata["schema_version"]

    def verify_schema(self) -> int:
        with self._connect() as connection:
            return self._verify_schema_connection(connection)

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
        try:
            rel = Path(os.path.relpath(absolute, self.project_root)).as_posix()
            if not rel.startswith(".."):
                return rel
        except ValueError:
            pass

        try:
            rel_raw = Path(os.path.relpath(absolute, self.raw_output_dir)).as_posix()
            if not rel_raw.startswith(".."):
                return (Path("data/raw") / rel_raw).as_posix()
        except ValueError:
            pass

        return absolute.as_posix()

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
            classification_policy_version=row["classification_policy_version"],
            classification_basis=row["classification_basis"],
            classification_updated_at=row["classification_updated_at"],
            next_recheck_after=row["next_recheck_after"],
            recheck_policy_version=row["recheck_policy_version"],
        )

    def get_date_state(self, market_date: date | str) -> DateSyncState | None:
        date_text = market_date.isoformat() if isinstance(market_date, date) else market_date
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM date_sync_state WHERE market_date = ?", (date_text,)
            ).fetchone()
        return None if row is None else self._state_from_row(row)

    def get_date_states_for_range(
        self, start_date: str, end_date: str
    ) -> dict[str, DateSyncState]:
        """Load tracked state for a range without materializing missing dates."""

        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM date_sync_state
                WHERE market_date BETWEEN ? AND ?
                ORDER BY market_date
                """,
                (start_date, end_date),
            ).fetchall()
        return {row["market_date"]: self._state_from_row(row) for row in rows}

    def get_attempt_evidence_for_range(
        self, start_date: str, end_date: str
    ) -> dict[str, AttemptEvidenceRecord]:
        """Reconstruct immutable network evidence with one range query."""

        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM download_attempts
                WHERE market_date BETWEEN ? AND ?
                ORDER BY market_date, id
                """,
                (start_date, end_date),
            ).fetchall()

        grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for row in rows:
            grouped[row["market_date"]].append(row)

        evidence: dict[str, AttemptEvidenceRecord] = {}
        for market_date, attempts in grouped.items():
            empty_attempts = [
                row
                for row in attempts
                if row["http_status"] == 200
                and row["response_classification"]
                == "EMPTY_MARKET_RESPONSE"
                and row["final_status"]
                in {
                    DownloadStatus.EMPTY_MARKET_RESPONSE.value,
                    DownloadStatus.NON_TRADING_OR_EMPTY.value,
                }
            ]
            valid_attempts = [
                row
                for row in attempts
                if row["response_classification"] == "EQUITY_ROWS"
                and row["valid_row_count"] > 0
            ]
            empty_by_run: dict[str, str] = {}
            for row in empty_attempts:
                empty_by_run.setdefault(row["run_id"], row["finished_at"])
            latest_valid = valid_attempts[-1] if valid_attempts else None
            evidence[market_date] = AttemptEvidenceRecord(
                market_date=market_date,
                attempt_count=len(attempts),
                empty_observation_count=len(empty_attempts),
                independent_empty_run_count=len(empty_by_run),
                valid_observation_count=len(valid_attempts),
                independent_valid_run_count=len(
                    {row["run_id"] for row in valid_attempts}
                ),
                http_statuses=tuple(
                    row["http_status"]
                    for row in attempts
                    if row["http_status"] is not None
                ),
                response_classifications=tuple(
                    row["response_classification"]
                    for row in attempts
                    if row["response_classification"] is not None
                ),
                first_observed_at=attempts[0]["started_at"],
                last_observed_at=attempts[-1]["finished_at"],
                empty_run_observations=tuple(empty_by_run.items()),
                latest_valid_checksum=(
                    None if latest_valid is None else latest_valid["checksum"]
                ),
                latest_valid_relative_path=(
                    None
                    if latest_valid is None
                    else latest_valid["csv_relative_path"]
                ),
            )
        return evidence

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
        if event.final_status is DownloadStatus.REPAIR_REQUIRED:
            raise StateDatabaseError(
                "a local repair-required outcome cannot be recorded as an HTTP attempt"
            )
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
            contradictory_trading_observation = (
                current.status is PersistentSyncStatus.CONFIRMED_NON_TRADING
                and event.response_classification == "EQUITY_ROWS"
                and event.valid_row_count > 0
            )
            trusted_historical_identity = _has_trusted_artifact_identity(current)
            raced_successful_identity = successful and trusted_historical_identity
            raced_success_conflict = (
                raced_successful_identity
                and not _matches_trusted_artifact_identity(
                    current,
                    checksum=event.checksum,
                    row_count=event.valid_row_count,
                )
            )
            accepted_successful = successful and not raced_success_conflict
            preserve_non_promoted_identity = (
                trusted_historical_identity and not accepted_successful
            )
            preserve_artifact_status = (
                current.status in ARTIFACT_ISSUE_STATUSES
                and not accepted_successful
            )
            if raced_success_conflict:
                status = PersistentSyncStatus.FILE_CONFLICT
            elif contradictory_trading_observation:
                status = incoming
            elif preserve_artifact_status:
                status = current.status
            else:
                status = resolve_status_transition(current.status, incoming)
            preserve_historical_counts = (
                raced_successful_identity
                or preserve_non_promoted_identity
                or (
                    current.status in VERIFIED_STATUSES
                    and incoming in TRANSIENT_STATUSES
                    and status is current.status
                )
            )
            parsed_row_count = (
                current.parsed_row_count
                if preserve_historical_counts
                else event.parsed_row_count
            )
            valid_row_count = (
                current.valid_row_count
                if preserve_historical_counts
                else event.valid_row_count
            )
            rejected_row_count = (
                current.rejected_row_count
                if preserve_historical_counts
                else event.rejected_row_count
            )
            if raced_success_conflict:
                evidence_state = SyncEvidenceState.LOCAL_CHECKSUM_CONFLICT
                error_type = "ARTIFACT_IDENTITY_CONFLICT"
                error_message = (
                    "successful network candidate conflicts with a trusted "
                    "artifact identity established while the fetch was in flight"
                )
            elif accepted_successful:
                evidence_state = SyncEvidenceState.NETWORK_VALIDATED_CSV
                error_type = None
                error_message = None
            elif preserve_historical_counts:
                evidence_state = current.evidence_state
                error_type = event.error_type
                error_message = _clean_error(event.error_message)
            else:
                evidence_state = SyncEvidenceState.NETWORK_OBSERVATION
                error_type = event.error_type
                error_message = _clean_error(event.error_message)
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
                    csv_checksum_sha256 = CASE
                        WHEN ? THEN csv_checksum_sha256
                        ELSE COALESCE(?, csv_checksum_sha256) END,
                    csv_relative_path = CASE
                        WHEN ? THEN csv_relative_path
                        ELSE COALESCE(?, csv_relative_path) END,
                    first_attempt_at = COALESCE(first_attempt_at, ?),
                    last_attempt_at = ?,
                    last_success_at = CASE WHEN ? THEN ? ELSE last_success_at END,
                    last_verified_at = CASE WHEN ? THEN ? ELSE last_verified_at END,
                    last_error_type = ?,
                    last_error_message = ?,
                    last_duration_ms = ?,
                    source_endpoint = ?,
                    classification_policy_version = CASE
                        WHEN ? THEN classification_policy_version ELSE NULL END,
                    classification_basis = CASE
                        WHEN ? THEN classification_basis ELSE NULL END,
                    classification_updated_at = CASE
                        WHEN ? THEN classification_updated_at ELSE NULL END,
                    next_recheck_after = NULL,
                    recheck_policy_version = NULL,
                    record_updated_at = ?
                WHERE market_date = ?
                """,
                (
                    status.value,
                    evidence_state.value,
                    int(accepted_successful),
                    event.http_status,
                    event.response_bytes,
                    parsed_row_count,
                    valid_row_count,
                    rejected_row_count,
                    int(preserve_historical_counts),
                    event.checksum,
                    int(preserve_historical_counts),
                    relative_path,
                    event.started_at,
                    event.finished_at,
                    int(accepted_successful),
                    event.finished_at,
                    int(accepted_successful),
                    event.finished_at,
                    error_type,
                    error_message,
                    event.duration_ms,
                    self.source_endpoint,
                    int(status is PersistentSyncStatus.CONFIRMED_NON_TRADING),
                    int(status is PersistentSyncStatus.CONFIRMED_NON_TRADING),
                    int(status is PersistentSyncStatus.CONFIRMED_NON_TRADING),
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

    def record_staged_attempt(
        self, run_id: str, event: DownloadAttemptEvent
    ) -> None:
        """Audit a staged HTTP attempt without replacing canonical identity."""

        now = utc_now_iso()
        relative_path = self._relative_path(event.saved_path)
        incoming = persistent_status_for_download(event.final_status)
        successful = event.final_status in {
            DownloadStatus.TRADING_DATA,
            DownloadStatus.ALREADY_PRESENT,
        }
        with self._connect() as connection:
            self._ensure_date_state(connection, event.requested_date, now)
            row = connection.execute(
                "SELECT * FROM date_sync_state WHERE market_date = ?",
                (event.requested_date,),
            ).fetchone()
            assert row is not None
            current = self._state_from_row(row)
            historical_identity = (
                current.last_verified_at is not None
                or current.csv_checksum_sha256 is not None
                or current.status
                in {
                    *VERIFIED_STATUSES,
                    PersistentSyncStatus.FILE_MISSING,
                    PersistentSyncStatus.FILE_CORRUPT,
                    PersistentSyncStatus.FILE_CONFLICT,
                }
            )
            contradictory_trading_observation = (
                current.status is PersistentSyncStatus.CONFIRMED_NON_TRADING
                and event.response_classification == "EQUITY_ROWS"
                and event.valid_row_count > 0
            )
            preserve_artifact_identity = (
                historical_identity and not contradictory_trading_observation
            )
            preserve_classification = (
                (historical_identity or successful)
                and not contradictory_trading_observation
            )
            status = (
                incoming
                if contradictory_trading_observation
                else (
                    current.status
                    if preserve_classification
                    else resolve_status_transition(current.status, incoming)
                )
            )
            evidence_state = (
                current.evidence_state
                if preserve_classification
                else SyncEvidenceState.NETWORK_OBSERVATION
            )
            parsed_rows = (
                current.parsed_row_count
                if preserve_artifact_identity
                else event.parsed_row_count
            )
            valid_rows = (
                current.valid_row_count
                if preserve_artifact_identity
                else event.valid_row_count
            )
            rejected_rows = (
                current.rejected_row_count
                if preserve_artifact_identity
                else event.rejected_row_count
            )
            connection.execute(
                """
                UPDATE date_sync_state SET
                    status = ?, evidence_state = ?,
                    attempt_count = attempt_count + 1,
                    successful_attempt_count = successful_attempt_count + ?,
                    last_http_status = ?, last_response_bytes = ?,
                    parsed_row_count = ?, valid_row_count = ?,
                    rejected_row_count = ?,
                    first_attempt_at = COALESCE(first_attempt_at, ?),
                    last_attempt_at = ?,
                    last_success_at = CASE WHEN ? THEN ? ELSE last_success_at END,
                    last_error_type = ?, last_error_message = ?,
                    last_duration_ms = ?, source_endpoint = ?,
                    classification_policy_version = CASE
                        WHEN ? THEN classification_policy_version ELSE NULL END,
                    classification_basis = CASE
                        WHEN ? THEN classification_basis ELSE NULL END,
                    classification_updated_at = CASE
                        WHEN ? THEN classification_updated_at ELSE NULL END,
                    next_recheck_after = NULL,
                    recheck_policy_version = NULL,
                    record_updated_at = ?
                WHERE market_date = ?
                """,
                (
                    status.value,
                    evidence_state.value,
                    int(successful),
                    event.http_status,
                    event.response_bytes,
                    parsed_rows,
                    valid_rows,
                    rejected_rows,
                    event.started_at,
                    event.finished_at,
                    int(successful),
                    event.finished_at,
                    None if successful else event.error_type,
                    None if successful else _clean_error(event.error_message),
                    event.duration_ms,
                    self.source_endpoint,
                    int(status is PersistentSyncStatus.CONFIRMED_NON_TRADING),
                    int(status is PersistentSyncStatus.CONFIRMED_NON_TRADING),
                    int(status is PersistentSyncStatus.CONFIRMED_NON_TRADING),
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

    def record_staged_download_result(
        self, run_id: str, result: DownloadResult
    ) -> None:
        """Store a run result while leaving canonical date state untouched."""

        now = utc_now_iso()
        relative_path = self._relative_path(result.saved_path)
        with self._connect() as connection:
            self._ensure_date_state(connection, result.requested_date, now)
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

    def record_download_result(self, run_id: str, result: DownloadResult) -> None:
        now = utc_now_iso()
        relative_path = self._relative_path(result.saved_path)
        successful = result.successful
        with self._connect() as connection:
            self._ensure_date_state(connection, result.requested_date, now)
            current_row = connection.execute(
                "SELECT * FROM date_sync_state WHERE market_date = ?",
                (result.requested_date,),
            ).fetchone()
            assert current_row is not None
            current = self._state_from_row(current_row)
            local_artifact_outcome = (
                result.attempts == 0
                and result.status in LOCAL_ARTIFACT_DOWNLOAD_STATUSES
            )
            incoming = (
                current.status
                if result.status is DownloadStatus.REPAIR_REQUIRED
                else persistent_status_for_download(result.status)
            )
            contradictory_trading_observation = (
                current.status is PersistentSyncStatus.CONFIRMED_NON_TRADING
                and result.valid_row_count > 0
            )
            trusted_historical_identity = _has_trusted_artifact_identity(current)
            raced_successful_identity = successful and trusted_historical_identity
            raced_success_conflict = (
                raced_successful_identity
                and not _matches_trusted_artifact_identity(
                    current,
                    checksum=result.checksum,
                    row_count=result.valid_row_count,
                )
            )
            accepted_successful = successful and not raced_success_conflict
            preserve_non_promoted_identity = (
                trusted_historical_identity and not accepted_successful
            )
            preserve_artifact_status = (
                current.status in ARTIFACT_ISSUE_STATUSES
                and not accepted_successful
            )
            if raced_success_conflict:
                status = PersistentSyncStatus.FILE_CONFLICT
            elif contradictory_trading_observation:
                status = incoming
            elif local_artifact_outcome or preserve_artifact_status:
                status = current.status
            else:
                status = resolve_status_transition(current.status, incoming)
            preserve_historical_counts = (
                local_artifact_outcome
                or raced_successful_identity
                or preserve_non_promoted_identity
                or (
                    current.status in VERIFIED_STATUSES
                    and incoming in TRANSIENT_STATUSES
                    and status is current.status
                )
            )
            parsed_row_count = (
                current.parsed_row_count
                if preserve_historical_counts
                else result.parsed_row_count
            )
            valid_row_count = (
                current.valid_row_count
                if preserve_historical_counts
                else result.valid_row_count
            )
            rejected_row_count = (
                current.rejected_row_count
                if preserve_historical_counts
                else result.rejected_row_count
            )
            if raced_success_conflict:
                evidence_state = SyncEvidenceState.LOCAL_CHECKSUM_CONFLICT
            elif accepted_successful:
                evidence_state = (
                    SyncEvidenceState.LOCAL_CSV_SHA256_VERIFIED
                    if result.locally_skipped
                    else SyncEvidenceState.NETWORK_VALIDATED_CSV
                )
            elif preserve_historical_counts:
                evidence_state = current.evidence_state
            elif incoming is PersistentSyncStatus.CONFIRMED_NON_TRADING:
                evidence_state = current.evidence_state
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
            clear_recheck = accepted_successful or result.attempts > 0
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
                    csv_checksum_sha256 = CASE
                        WHEN ? THEN csv_checksum_sha256
                        ELSE COALESCE(?, csv_checksum_sha256) END,
                    csv_relative_path = CASE
                        WHEN ? THEN csv_relative_path
                        ELSE COALESCE(?, csv_relative_path) END,
                    last_success_at = CASE
                        WHEN ? THEN COALESCE(last_success_at, ?) ELSE last_success_at END,
                    last_verified_at = CASE WHEN ? THEN ? ELSE last_verified_at END,
                    last_error_type = ?,
                    last_error_message = ?,
                    last_duration_ms = ?,
                    source_endpoint = ?,
                    classification_policy_version = CASE
                        WHEN ? THEN classification_policy_version ELSE NULL END,
                    classification_basis = CASE
                        WHEN ? THEN classification_basis ELSE NULL END,
                    classification_updated_at = CASE
                        WHEN ? THEN classification_updated_at ELSE NULL END,
                    next_recheck_after = CASE
                        WHEN ? THEN NULL ELSE next_recheck_after END,
                    recheck_policy_version = CASE
                        WHEN ? THEN NULL ELSE recheck_policy_version END,
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
                    int(preserve_historical_counts),
                    result.checksum,
                    int(preserve_historical_counts),
                    relative_path,
                    int(accepted_successful),
                    now,
                    int(accepted_successful),
                    now,
                    (
                        "ARTIFACT_IDENTITY_CONFLICT"
                        if raced_success_conflict
                        else None
                        if accepted_successful
                        or incoming is PersistentSyncStatus.CONFIRMED_NON_TRADING
                        else result.status.value
                    ),
                    (
                        (
                            "successful network candidate conflicts with a trusted "
                            "artifact identity established while the fetch was in flight"
                        )
                        if raced_success_conflict
                        else None
                        if accepted_successful
                        or incoming is PersistentSyncStatus.CONFIRMED_NON_TRADING
                        else _clean_error(result.error)
                    ),
                    result.elapsed_ms,
                    source_endpoint,
                    int(status is PersistentSyncStatus.CONFIRMED_NON_TRADING),
                    int(status is PersistentSyncStatus.CONFIRMED_NON_TRADING),
                    int(status is PersistentSyncStatus.CONFIRMED_NON_TRADING),
                    int(clear_recheck),
                    int(clear_recheck),
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
        expected_record_updated_at: str | None = None,
        expected_state_exists: bool | None = None,
        reconciliation_run_id: str | None = None,
        reconciliation_decision: DateReconciliationResult | None = None,
        expected_artifact_path: Path | None = None,
        expected_artifact_exists: bool | None = None,
        expected_artifact_valid: bool | None = None,
        expected_observed_checksum: str | None = None,
    ) -> bool:
        if (reconciliation_run_id is None) != (reconciliation_decision is None):
            raise StateDatabaseError(
                "reconciliation run and decision must be supplied together"
            )
        now = utc_now_iso()
        relative_path = self._relative_path(path)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if expected_artifact_path is not None:
                guarded = inspect_canonical_csv_file(
                    expected_artifact_path, CANONICAL_COLUMNS
                )
                if (
                    expected_artifact_exists is not None
                    and guarded.exists != expected_artifact_exists
                ):
                    raise StateDatabaseError(
                        f"artifact changed after reconciliation planning for "
                        f"{market_date}"
                    )
                if (
                    expected_artifact_valid is not None
                    and guarded.valid != expected_artifact_valid
                ):
                    raise StateDatabaseError(
                        f"artifact validity changed after planning for {market_date}"
                    )
                if (
                    expected_observed_checksum is not None
                    and guarded.checksum != expected_observed_checksum
                ):
                    raise StateDatabaseError(
                        f"artifact checksum changed after planning for {market_date}"
                    )
            row = connection.execute(
                "SELECT * FROM date_sync_state WHERE market_date = ?", (market_date,)
            ).fetchone()
            if expected_state_exists is not None and (
                (row is not None) != expected_state_exists
            ):
                raise StateDatabaseError(
                    f"state changed after reconciliation planning for {market_date}"
                )
            self._ensure_date_state(connection, market_date, now)
            if row is None:
                row = connection.execute(
                    "SELECT * FROM date_sync_state WHERE market_date = ?",
                    (market_date,),
                ).fetchone()
            assert row is not None
            current = self._state_from_row(row)
            if (
                expected_record_updated_at is not None
                and current.record_updated_at != expected_record_updated_at
            ):
                raise StateDatabaseError(
                    f"stale reconciliation plan for {market_date}"
                )
            resolved = resolve_status_transition(current.status, status)
            if reconciliation_run_id is not None:
                assert reconciliation_decision is not None
                _validate_reconciliation_mutation_event(
                    connection,
                    reconciliation_run_id,
                    reconciliation_decision,
                    market_date=market_date,
                    new_status=resolved,
                )
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
                    (
                        resolved is not PersistentSyncStatus.CONFIRMED_NON_TRADING
                        and any(
                            (
                                current.classification_policy_version,
                                current.classification_basis,
                                current.classification_updated_at,
                            )
                        )
                    ),
                    (
                        resolved in VERIFIED_STATUSES
                        and any(
                            (
                                current.next_recheck_after,
                                current.recheck_policy_version,
                            )
                        )
                    ),
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
                    classification_policy_version = CASE
                        WHEN ? THEN classification_policy_version ELSE NULL END,
                    classification_basis = CASE
                        WHEN ? THEN classification_basis ELSE NULL END,
                    classification_updated_at = CASE
                        WHEN ? THEN classification_updated_at ELSE NULL END,
                    next_recheck_after = CASE
                        WHEN ? THEN NULL ELSE next_recheck_after END,
                    recheck_policy_version = CASE
                        WHEN ? THEN NULL ELSE recheck_policy_version END,
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
                    int(resolved is PersistentSyncStatus.CONFIRMED_NON_TRADING),
                    int(resolved is PersistentSyncStatus.CONFIRMED_NON_TRADING),
                    int(resolved is PersistentSyncStatus.CONFIRMED_NON_TRADING),
                    int(verified),
                    int(verified),
                    current.source_endpoint,
                    now,
                    market_date,
                ),
            )
            if reconciliation_run_id is not None:
                assert reconciliation_decision is not None
                _insert_reconciliation_event(
                    connection,
                    reconciliation_run_id,
                    reconciliation_decision,
                )
            return True

    def prepare_fetch(
        self,
        market_date: date,
        output_dir: Path,
        columns: tuple[str, ...] = CANONICAL_COLUMNS,
        *,
        expected_record_updated_at: str | None = None,
        expected_state_exists: bool | None = None,
        reconciliation_run_id: str | None = None,
        reconciliation_decision: DateReconciliationResult | None = None,
        expected_artifact_path: Path | None = None,
        expected_artifact_exists: bool | None = None,
        expected_artifact_valid: bool | None = None,
        expected_observed_checksum: str | None = None,
        allow_staged_repair: bool = False,
        mutate_state: bool = True,
    ) -> DownloadResult | None:
        """Central skip/corruption policy; return a local outcome or allow HTTP."""

        started = time.perf_counter()
        date_text = market_date.isoformat()
        current = self.get_date_state(date_text)
        inspection = inspect_existing_canonical_file(market_date, output_dir, columns)
        _validate_artifact_snapshot(
            inspection,
            market_date=date_text,
            expected_artifact_path=expected_artifact_path,
            expected_artifact_exists=expected_artifact_exists,
            expected_artifact_valid=expected_artifact_valid,
            expected_observed_checksum=expected_observed_checksum,
        )
        elapsed_ms = lambda: (time.perf_counter() - started) * 1000

        def repair_required(issue: str) -> DownloadResult:
            command = (
                "python -m psx_data_sync.cli reconcile "
                f"--start {date_text} --end {date_text} --apply"
            )
            return DownloadResult(
                requested_date=date_text,
                status=DownloadStatus.REPAIR_REQUIRED,
                elapsed_ms=elapsed_ms(),
                locally_skipped=True,
                warnings=(
                    "ordinary fetch cannot write directly into a canonical "
                    "artifact with persistent repair history",
                ),
                error=f"{issue}; run `{command}` for staged audited repair",
            )

        if inspection.valid:
            has_verified_identity = _has_trusted_artifact_identity(current)
            if (
                current is not None
                and current.status is PersistentSyncStatus.FILE_CONFLICT
                and not has_verified_identity
            ):
                message = (
                    "persistent file conflict lacks a trusted historical identity; "
                    "explicit audited resolution is required"
                )
                return DownloadResult(
                    requested_date=date_text,
                    status=DownloadStatus.FILE_CONFLICT,
                    elapsed_ms=elapsed_ms(),
                    locally_skipped=not mutate_state,
                    error=message,
                )
            if has_verified_identity:
                assert current is not None
                if current.csv_checksum_sha256 != inspection.checksum:
                    message = (
                        "verified database checksum differs from the valid canonical "
                        "CSV; automatic replacement is disabled"
                    )
                    if mutate_state:
                        self._set_artifact_state(
                            date_text,
                            PersistentSyncStatus.FILE_CONFLICT,
                            error_type="CHECKSUM_MISMATCH",
                            error_message=message,
                            preserve_artifact=True,
                            expected_record_updated_at=expected_record_updated_at,
                            expected_state_exists=expected_state_exists,
                            reconciliation_run_id=reconciliation_run_id,
                            reconciliation_decision=reconciliation_decision,
                            expected_artifact_path=inspection.path,
                            expected_artifact_exists=True,
                            expected_artifact_valid=True,
                            expected_observed_checksum=inspection.checksum,
                        )
                    if not allow_staged_repair:
                        return repair_required(message)
                    return DownloadResult(
                        requested_date=date_text,
                        status=DownloadStatus.FILE_CONFLICT,
                        elapsed_ms=elapsed_ms(),
                        locally_skipped=not mutate_state,
                        error=message,
                    )
            if mutate_state:
                self._set_artifact_state(
                    date_text,
                    PersistentSyncStatus.VERIFIED_TRADING_DATA,
                    row_count=inspection.row_count,
                    checksum=inspection.checksum,
                    path=inspection.path,
                    expected_record_updated_at=expected_record_updated_at,
                    expected_state_exists=expected_state_exists,
                    reconciliation_run_id=reconciliation_run_id,
                    reconciliation_decision=reconciliation_decision,
                    expected_artifact_path=inspection.path,
                    expected_artifact_exists=True,
                    expected_artifact_valid=True,
                    expected_observed_checksum=inspection.checksum,
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
            has_verified_identity = _has_trusted_artifact_identity(current)
            if mutate_state:
                self._set_artifact_state(
                    date_text,
                    PersistentSyncStatus.FILE_CORRUPT,
                    path=inspection.path,
                    error_type="FILE_CORRUPT",
                    error_message=message,
                    preserve_artifact=has_verified_identity,
                    expected_record_updated_at=expected_record_updated_at,
                    expected_state_exists=expected_state_exists,
                    reconciliation_run_id=reconciliation_run_id,
                    reconciliation_decision=reconciliation_decision,
                    expected_artifact_path=inspection.path,
                    expected_artifact_exists=True,
                    expected_artifact_valid=False,
                    expected_observed_checksum=inspection.checksum,
                )
            if not allow_staged_repair and (
                has_verified_identity
                or (
                    current is not None
                    and current.status in ARTIFACT_ISSUE_STATUSES
                )
            ):
                return repair_required(message)
            return DownloadResult(
                requested_date=date_text,
                status=DownloadStatus.EXISTING_FILE_INVALID,
                elapsed_ms=elapsed_ms(),
                locally_skipped=not mutate_state,
                error=message,
            )

        if (
            current is not None
            and current.status is PersistentSyncStatus.CONFIRMED_NON_TRADING
            and current.classification_policy_version
            == RECONCILIATION_POLICY_VERSION
            and current.classification_basis
            == WEEKEND_EMPTY_CLASSIFICATION_BASIS
        ):
            return DownloadResult(
                requested_date=date_text,
                status=DownloadStatus.CONFIRMED_NON_TRADING,
                elapsed_ms=elapsed_ms(),
                locally_skipped=True,
                warnings=(
                    "versioned non-trading conclusion verified; PSX request skipped",
                ),
            )

        trusted_identity = _has_trusted_artifact_identity(current)
        persisted_artifact_issue = (
            current is not None and current.status in ARTIFACT_ISSUE_STATUSES
        )
        verified_state = (
            current is not None and current.status in VERIFIED_STATUSES
        )
        if current is not None and (verified_state or trusted_identity):
            if mutate_state and not persisted_artifact_issue:
                self._set_artifact_state(
                    date_text,
                    PersistentSyncStatus.FILE_MISSING,
                    error_type="FILE_MISSING",
                    error_message=(
                        "verified CSV is missing; staged audited repair is required"
                    ),
                    preserve_artifact=True,
                    expected_record_updated_at=expected_record_updated_at,
                    expected_state_exists=expected_state_exists,
                    reconciliation_run_id=reconciliation_run_id,
                    reconciliation_decision=reconciliation_decision,
                    expected_artifact_path=inspection.path,
                    expected_artifact_exists=False,
                    expected_artifact_valid=False,
                )
            if not allow_staged_repair:
                return repair_required(
                    "trusted canonical CSV is missing or unresolved"
                )
        elif persisted_artifact_issue and not allow_staged_repair:
            return repair_required("canonical artifact issue remains unresolved")
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
                    current is not None
                    and current.status is PersistentSyncStatus.FILE_CONFLICT
                    and not has_verified_identity
                ):
                    message = (
                        "persistent file conflict lacks a trusted historical "
                        "identity; bootstrap did not replace its identity"
                    )
                    changed = False
                    outcome_status = PersistentSyncStatus.FILE_CONFLICT
                    outcome_error = message
                elif (
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

    def index_local_file(
        self,
        path: Path,
        columns: tuple[str, ...] = CANONICAL_COLUMNS,
    ) -> PersistentSyncStatus:
        """Index a single local canonical CSV file into date_sync_state safely."""

        path = Path(path)
        match = MARKET_FILE_PATTERN.fullmatch(path.name)
        if match is None:
            raise StateDatabaseError(
                f"cannot index file with unsupported name: {path.name}"
            )
        date_text = match.group(1)
        try:
            market_date = date.fromisoformat(date_text)
        except ValueError as exc:
            raise StateDatabaseError(
                f"cannot index file with invalid calendar date: {path.name}"
            ) from exc

        inspection = inspect_existing_canonical_file(
            market_date, path.parent, columns=columns
        )
        if not inspection.valid:
            message = inspection.error or "invalid canonical CSV"
            self._set_artifact_state(
                date_text,
                PersistentSyncStatus.FILE_CORRUPT,
                path=inspection.path,
                error_type="FILE_CORRUPT",
                error_message=message,
            )
            return PersistentSyncStatus.FILE_CORRUPT

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
                "verified database checksum differs from canonical CSV; "
                "state identity preserved"
            )
            self._set_artifact_state(
                date_text,
                PersistentSyncStatus.FILE_CONFLICT,
                error_type="CHECKSUM_MISMATCH",
                error_message=message,
                preserve_artifact=True,
            )
            return PersistentSyncStatus.FILE_CONFLICT

        self._set_artifact_state(
            date_text,
            PersistentSyncStatus.VERIFIED_TRADING_DATA,
            row_count=inspection.row_count,
            checksum=inspection.checksum,
            path=inspection.path,
        )
        return PersistentSyncStatus.VERIFIED_TRADING_DATA

    def get_dashboard_summary(self) -> DashboardSummary:
        """Fetch read-only summary metrics for GUI dashboard presentation."""

        with self._connect() as connection:
            range_row = connection.execute(
                """
                SELECT COUNT(*) AS total,
                       MIN(market_date) AS earliest,
                       MAX(market_date) AS latest
                FROM date_sync_state
                """
            ).fetchone()

            total_tracked = range_row["total"] if range_row else 0
            earliest = range_row["earliest"] if range_row else None
            latest = range_row["latest"] if range_row else None

            status_rows = connection.execute(
                """
                SELECT status, COUNT(*) AS cnt
                FROM date_sync_state
                GROUP BY status
                """
            ).fetchall()
            status_counts = {row["status"]: row["cnt"] for row in status_rows}

            verified_trading = status_counts.get(
                PersistentSyncStatus.VERIFIED_TRADING_DATA.value, 0
            )
            confirmed_non_trading = status_counts.get(
                PersistentSyncStatus.CONFIRMED_NON_TRADING.value, 0
            )
            empty_unresolved = status_counts.get(
                PersistentSyncStatus.EMPTY_UNRESOLVED.value, 0
            )

            file_issue_statuses = {
                PersistentSyncStatus.FILE_CORRUPT.value,
                PersistentSyncStatus.FILE_MISSING.value,
                PersistentSyncStatus.FILE_CONFLICT.value,
            }
            file_issue_count = sum(
                status_counts.get(st, 0) for st in file_issue_statuses
            )

            failure_statuses = {
                PersistentSyncStatus.TEMPORARY_FAILURE.value,
                PersistentSyncStatus.HTTP_FAILURE.value,
                PersistentSyncStatus.PARSE_FAILURE.value,
                PersistentSyncStatus.VALIDATION_FAILURE.value,
            }
            failure_count = sum(
                status_counts.get(st, 0) for st in failure_statuses
            )

            local_evidence_row = connection.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM date_sync_state
                WHERE evidence_state = ?
                """,
                (SyncEvidenceState.LOCAL_CSV_SHA256_VERIFIED.value,),
            ).fetchone()
            local_csv_verified = (
                local_evidence_row["cnt"] if local_evidence_row else 0
            )

            parquet_rows = connection.execute(
                """
                SELECT status, COUNT(*) AS cnt
                FROM parquet_exports
                GROUP BY status
                """
            ).fetchall()
            parquet_counts = {row["status"]: row["cnt"] for row in parquet_rows}

            parquet_current = parquet_counts.get(
                ParquetExportStatus.CURRENT.value, 0
            )
            parquet_missing = parquet_counts.get(
                ParquetExportStatus.MISSING.value, 0
            )
            parquet_stale = parquet_counts.get(
                ParquetExportStatus.STALE.value, 0
            )
            parquet_corrupt = parquet_counts.get(
                ParquetExportStatus.CORRUPT.value, 0
            )
            parquet_failed = parquet_counts.get(
                ParquetExportStatus.FAILED.value, 0
            )

        raw_dir = self.project_root / "data" / "raw"
        canonical_csv_count = (
            len(list(raw_dir.glob("market_*.csv"))) if raw_dir.exists() else 0
        )

        return DashboardSummary(
            application_version=self.application_version,
            schema_version=SCHEMA_VERSION,
            database_path=self.database_path,
            total_tracked_dates=total_tracked,
            earliest_date=earliest,
            latest_date=latest,
            verified_trading_count=verified_trading,
            local_csv_verified_count=local_csv_verified,
            confirmed_non_trading_count=confirmed_non_trading,
            empty_unresolved_count=empty_unresolved,
            file_issue_count=file_issue_count,
            failure_count=failure_count,
            parquet_current_count=parquet_current,
            parquet_missing_count=parquet_missing,
            parquet_stale_count=parquet_stale,
            parquet_corrupt_count=parquet_corrupt,
            parquet_failed_count=parquet_failed,
            total_canonical_csv_count=canonical_csv_count,
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
                    DownloadStatus.CONFIRMED_NON_TRADING,
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

    def begin_reconciliation_run(
        self,
        *,
        policy_version: str,
        start_date: str,
        end_date: str,
        mode: ReconciliationMode,
        requested_date_count: int,
        worker_count: int,
        force_recheck: bool,
        max_rechecks_per_date: int,
        cooldown_seconds: float = 86_400.0,
    ) -> str:
        run_id = uuid.uuid4().hex
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO reconciliation_runs (
                    run_id, policy_version, start_date, end_date, mode,
                    requested_date_count, worker_count, force_recheck,
                    max_rechecks_per_date, cooldown_seconds, started_at,
                    interrupted, status,
                    application_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                """,
                (
                    run_id,
                    policy_version,
                    start_date,
                    end_date,
                    mode.value,
                    requested_date_count,
                    worker_count,
                    int(force_recheck),
                    max_rechecks_per_date,
                    cooldown_seconds,
                    utc_now_iso(),
                    ReconciliationRunStatus.RUNNING.value,
                    self.application_version,
                ),
            )
        return run_id

    def link_reconciliation_sync_run(
        self, reconciliation_run_id: str, sync_run_id: str
    ) -> None:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE reconciliation_runs SET linked_sync_run_id = ?
                WHERE run_id = ?
                """,
                (sync_run_id, reconciliation_run_id),
            )
            if cursor.rowcount != 1:
                raise StateDatabaseError(
                    f"unknown reconciliation run: {reconciliation_run_id}"
                )

    def finish_reconciliation_run(
        self,
        result: ReconciliationRangeResult,
        *,
        interrupted: bool = False,
        failed: bool = False,
        error_message: str | None = None,
    ) -> ReconciliationRunRecord:
        if interrupted and failed:
            raise ValueError("a reconciliation run cannot be failed and interrupted")
        finished_at = utc_now_iso()
        run_status = (
            ReconciliationRunStatus.INTERRUPTED
            if interrupted
            else (
                ReconciliationRunStatus.FAILED
                if failed
                else ReconciliationRunStatus.COMPLETED
            )
        )
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE reconciliation_runs SET
                    verified_count = ?, confirmed_non_trading_count = ?,
                    never_attempted_count = ?, unresolved_count = ?,
                    failure_count = ?, file_health_issue_count = ?,
                    network_recheck_planned_count = ?, network_recheck_count = ?,
                    local_repair_count = ?, manual_review_count = ?,
                    status_transition_count = ?, complete = ?,
                    finished_at = ?, duration_ms = ?, interrupted = ?,
                    status = ?, error_message = ?
                WHERE run_id = ?
                """,
                (
                    result.verified_count,
                    result.confirmed_non_trading_count,
                    result.never_attempted_count,
                    result.unresolved_count,
                    result.failure_count,
                    result.file_health_issue_count,
                    result.network_recheck_planned_count,
                    result.network_recheck_count,
                    result.local_repair_count,
                    result.manual_review_count,
                    result.status_transition_count,
                    int(result.complete),
                    finished_at,
                    result.duration_ms,
                    int(interrupted),
                    run_status.value,
                    _clean_error(error_message),
                    result.run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise StateDatabaseError(
                    f"unknown reconciliation run: {result.run_id}"
                )
        record = self.get_reconciliation_run(result.run_id)
        assert record is not None
        return record

    def mark_reconciliation_run_failed(
        self,
        run_id: str,
        *,
        interrupted: bool,
        duration_ms: float,
        error_message: str | None,
    ) -> None:
        status = (
            ReconciliationRunStatus.INTERRUPTED
            if interrupted
            else ReconciliationRunStatus.FAILED
        )
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE reconciliation_runs SET
                    finished_at = ?, duration_ms = ?, interrupted = ?,
                    status = ?, error_message = ?
                WHERE run_id = ?
                """,
                (
                    utc_now_iso(),
                    duration_ms,
                    int(interrupted),
                    status.value,
                    _clean_error(error_message),
                    run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise StateDatabaseError(
                    f"unknown reconciliation run: {run_id}"
                )

    @staticmethod
    def _reconciliation_run_from_row(row: sqlite3.Row) -> ReconciliationRunRecord:
        return ReconciliationRunRecord(
            run_id=row["run_id"],
            policy_version=row["policy_version"],
            start_date=row["start_date"],
            end_date=row["end_date"],
            mode=ReconciliationMode(row["mode"]),
            requested_date_count=row["requested_date_count"],
            worker_count=row["worker_count"],
            force_recheck=bool(row["force_recheck"]),
            max_rechecks_per_date=row["max_rechecks_per_date"],
            cooldown_seconds=row["cooldown_seconds"],
            verified_count=row["verified_count"],
            confirmed_non_trading_count=row["confirmed_non_trading_count"],
            never_attempted_count=row["never_attempted_count"],
            unresolved_count=row["unresolved_count"],
            failure_count=row["failure_count"],
            file_health_issue_count=row["file_health_issue_count"],
            network_recheck_planned_count=row[
                "network_recheck_planned_count"
            ],
            network_recheck_count=row["network_recheck_count"],
            local_repair_count=row["local_repair_count"],
            manual_review_count=row["manual_review_count"],
            status_transition_count=row["status_transition_count"],
            complete=bool(row["complete"]),
            linked_sync_run_id=row["linked_sync_run_id"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            duration_ms=row["duration_ms"],
            interrupted=bool(row["interrupted"]),
            status=ReconciliationRunStatus(row["status"]),
            error_message=row["error_message"],
            application_version=row["application_version"],
        )

    def get_reconciliation_run(
        self, run_id: str
    ) -> ReconciliationRunRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM reconciliation_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return None if row is None else self._reconciliation_run_from_row(row)

    def record_reconciliation_event(
        self, run_id: str, result: DateReconciliationResult
    ) -> None:
        with self._connect() as connection:
            _insert_reconciliation_event(connection, run_id, result)

    def list_reconciliation_events(self, run_id: str) -> tuple[sqlite3.Row, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM reconciliation_events
                WHERE run_id = ? ORDER BY id
                """,
                (run_id,),
            ).fetchall()
        return tuple(rows)

    def confirm_non_trading(
        self,
        market_date: str,
        *,
        policy_version: str,
        classification_basis: str,
        expected_record_updated_at: str | None,
        expected_state_exists: bool | None = None,
        canonical_path: Path | None = None,
        reconciliation_run_id: str | None = None,
        reconciliation_decision: DateReconciliationResult | None = None,
    ) -> bool:
        """Persist a defensively revalidated policy conclusion."""

        if (reconciliation_run_id is None) != (reconciliation_decision is None):
            raise StateDatabaseError(
                "reconciliation run and decision must be supplied together"
            )
        try:
            parsed_date = date.fromisoformat(market_date)
        except ValueError as exc:
            raise StateDatabaseError(
                f"invalid market date for non-trading conclusion: {market_date}"
            ) from exc
        if policy_version != RECONCILIATION_POLICY_VERSION:
            raise StateDatabaseError(
                f"unsupported reconciliation policy: {policy_version}"
            )
        if classification_basis != WEEKEND_EMPTY_CLASSIFICATION_BASIS:
            raise StateDatabaseError(
                f"unsupported non-trading evidence basis: {classification_basis}"
            )
        if parsed_date.weekday() < 5:
            raise StateDatabaseError(
                f"policy v1 cannot confirm weekday {market_date}"
            )
        if canonical_path is None:
            raise StateDatabaseError(
                "canonical path is required for non-trading classification"
            )

        now = utc_now_iso()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if os.path.lexists(canonical_path):
                raise StateDatabaseError(
                    f"refusing non-trading classification while an expected "
                    f"artifact exists for {market_date}"
                )
            row = connection.execute(
                "SELECT * FROM date_sync_state WHERE market_date = ?",
                (market_date,),
            ).fetchone()
            if expected_state_exists is not None and (
                (row is not None) != expected_state_exists
            ):
                raise StateDatabaseError(
                    f"state changed after reconciliation planning for {market_date}"
                )
            self._ensure_date_state(connection, market_date, now)
            if row is None:
                row = connection.execute(
                    "SELECT * FROM date_sync_state WHERE market_date = ?",
                    (market_date,),
                ).fetchone()
            assert row is not None
            current = self._state_from_row(row)
            if (
                expected_record_updated_at is not None
                and current.record_updated_at != expected_record_updated_at
            ):
                raise StateDatabaseError(
                    f"stale reconciliation plan for {market_date}"
                )
            if (
                current.last_verified_at is not None
                or current.successful_attempt_count > 0
                or current.status in VERIFIED_STATUSES
            ):
                raise StateDatabaseError(
                    f"refusing non-trading classification for historically "
                    f"verified date {market_date}"
                )
            attempts = connection.execute(
                """
                SELECT run_id, finished_at, response_classification,
                       final_status, http_status, valid_row_count
                FROM download_attempts
                WHERE market_date = ? ORDER BY finished_at, id
                """,
                (market_date,),
            ).fetchall()
            if any(
                attempt["response_classification"] == "EQUITY_ROWS"
                and attempt["valid_row_count"] > 0
                for attempt in attempts
            ):
                raise StateDatabaseError(
                    f"trading observation contradicts non-trading conclusion "
                    f"for {market_date}"
                )
            empty_by_run: dict[str, datetime] = {}
            for attempt in attempts:
                if not (
                    attempt["http_status"] == 200
                    and attempt["response_classification"]
                    == "EMPTY_MARKET_RESPONSE"
                    and attempt["final_status"]
                    in {
                        DownloadStatus.EMPTY_MARKET_RESPONSE.value,
                        DownloadStatus.NON_TRADING_OR_EMPTY.value,
                    }
                ):
                    continue
                try:
                    observed = datetime.fromisoformat(attempt["finished_at"])
                except ValueError:
                    continue
                if observed.tzinfo is None:
                    observed = observed.replace(tzinfo=timezone.utc)
                empty_by_run.setdefault(
                    attempt["run_id"], observed.astimezone(timezone.utc)
                )
            accepted: list[datetime] = []
            for observed in sorted(empty_by_run.values()):
                if not accepted or observed - accepted[-1] >= timedelta(hours=24):
                    accepted.append(observed)
            if len(accepted) < 2:
                raise StateDatabaseError(
                    "policy v1 requires two distinct-run empty observations "
                    "at least 24 hours apart"
                )
            resolved = resolve_status_transition(
                current.status, PersistentSyncStatus.CONFIRMED_NON_TRADING
            )
            if reconciliation_run_id is not None:
                assert reconciliation_decision is not None
                _validate_reconciliation_mutation_event(
                    connection,
                    reconciliation_run_id,
                    reconciliation_decision,
                    market_date=market_date,
                    new_status=resolved,
                )
            cursor = connection.execute(
                """
                UPDATE date_sync_state SET
                    status = ?, evidence_state = ?,
                    parsed_row_count = 0, valid_row_count = 0,
                    rejected_row_count = 0,
                    csv_checksum_sha256 = NULL, csv_relative_path = NULL,
                    classification_policy_version = ?,
                    classification_basis = ?, classification_updated_at = ?,
                    next_recheck_after = NULL, recheck_policy_version = NULL,
                    last_error_type = NULL, last_error_message = NULL,
                    record_updated_at = ?
                WHERE market_date = ? AND record_updated_at = ?
                """,
                (
                    resolved.value,
                    SyncEvidenceState.REPEATED_EMPTY_WITH_WEEKEND_CALENDAR.value,
                    policy_version,
                    classification_basis,
                    now,
                    now,
                    market_date,
                    current.record_updated_at,
                ),
            )
            if cursor.rowcount != 1:
                raise StateDatabaseError(
                    f"concurrent state update while classifying {market_date}"
                )
            if reconciliation_run_id is not None:
                assert reconciliation_decision is not None
                _insert_reconciliation_event(
                    connection,
                    reconciliation_run_id,
                    reconciliation_decision,
                )
            return current.status is not resolved

    def set_recheck_after(
        self,
        market_date: str,
        next_recheck_after: str | None,
        policy_version: str,
        *,
        expected_record_updated_at: str | None = None,
        expected_status: PersistentSyncStatus | None = None,
    ) -> bool:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT status, record_updated_at FROM date_sync_state
                WHERE market_date = ?
                """,
                (market_date,),
            ).fetchone()
            if row is None:
                return False
            if (
                expected_record_updated_at is not None
                and row["record_updated_at"] != expected_record_updated_at
            ):
                return False
            if (
                expected_status is not None
                and row["status"] != expected_status.value
            ):
                return False
            cursor = connection.execute(
                """
                UPDATE date_sync_state SET
                    next_recheck_after = ?, recheck_policy_version = ?,
                    record_updated_at = ?
                WHERE market_date = ?
                """,
                (
                    next_recheck_after,
                    policy_version if next_recheck_after else None,
                    utc_now_iso(),
                    market_date,
                ),
            )
            return cursor.rowcount == 1

    def mark_reconciliation_date_cancelled(
        self,
        market_date: str,
        *,
        message: str = "reconciliation interrupted before the date completed",
    ) -> bool:
        """Make an attempted-but-unfinished date immediately resumable.

        Cancellation can occur during retry backoff, after the latest HTTP
        attempt has already persisted a normal cooldown-producing failure.
        This interruption marker is not another network attempt; it only
        records why the date did not finish and clears the derived cooldown.
        Resolved dates are never downgraded or annotated by this helper.
        """

        now = utc_now_iso()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE date_sync_state SET
                    last_error_type = 'CANCELLED', last_error_message = ?,
                    next_recheck_after = NULL, recheck_policy_version = NULL,
                    record_updated_at = ?
                WHERE market_date = ?
                  AND status IN (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _clean_error(message),
                    now,
                    market_date,
                    PersistentSyncStatus.NEVER_ATTEMPTED.value,
                    PersistentSyncStatus.EMPTY_UNRESOLVED.value,
                    PersistentSyncStatus.TEMPORARY_FAILURE.value,
                    PersistentSyncStatus.HTTP_FAILURE.value,
                    PersistentSyncStatus.PARSE_FAILURE.value,
                    PersistentSyncStatus.VALIDATION_FAILURE.value,
                    PersistentSyncStatus.FILE_MISSING.value,
                ),
            )
            return cursor.rowcount == 1

    def mark_artifact_issue(
        self,
        market_date: str,
        status: PersistentSyncStatus,
        *,
        error_type: str,
        error_message: str,
        expected_record_updated_at: str | None = None,
        expected_state_exists: bool | None = None,
        reconciliation_run_id: str | None = None,
        reconciliation_decision: DateReconciliationResult | None = None,
        expected_artifact_path: Path | None = None,
        expected_artifact_exists: bool | None = None,
        expected_artifact_valid: bool | None = None,
        expected_observed_checksum: str | None = None,
    ) -> bool:
        if status not in {
            PersistentSyncStatus.FILE_MISSING,
            PersistentSyncStatus.FILE_CORRUPT,
            PersistentSyncStatus.FILE_CONFLICT,
        }:
            raise ValueError("artifact issue status is required")
        return self._set_artifact_state(
            market_date,
            status,
            error_type=error_type,
            error_message=error_message,
            preserve_artifact=True,
            expected_record_updated_at=expected_record_updated_at,
            expected_state_exists=expected_state_exists,
            reconciliation_run_id=reconciliation_run_id,
            reconciliation_decision=reconciliation_decision,
            expected_artifact_path=expected_artifact_path,
            expected_artifact_exists=expected_artifact_exists,
            expected_artifact_valid=expected_artifact_valid,
            expected_observed_checksum=expected_observed_checksum,
        )

    def claim_network_rechecks(
        self,
        run_id: str,
        market_dates: Iterable[str],
        *,
        claimed_at: str,
        expires_at: str,
    ) -> tuple[str, ...]:
        claimed: list[str] = []
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM reconciliation_recheck_claims WHERE expires_at <= ?",
                (claimed_at,),
            )
            for market_date in market_dates:
                cursor = connection.execute(
                    """
                    INSERT INTO reconciliation_recheck_claims (
                        market_date, reconciliation_run_id, claimed_at, expires_at
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(market_date) DO NOTHING
                    """,
                    (market_date, run_id, claimed_at, expires_at),
                )
                if cursor.rowcount == 1:
                    claimed.append(market_date)
        return tuple(claimed)

    def release_network_rechecks(self, run_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                DELETE FROM reconciliation_recheck_claims
                WHERE reconciliation_run_id = ?
                """,
                (run_id,),
            )

    @staticmethod
    def _require_running_apply_reconciliation(
        connection: sqlite3.Connection,
        reconciliation_run_id: str,
        market_date: str,
    ) -> sqlite3.Row:
        try:
            parsed_date = date.fromisoformat(market_date)
        except ValueError as exc:
            raise StateDatabaseError(
                f"invalid repair-candidate market date: {market_date}"
            ) from exc
        run = connection.execute(
            "SELECT * FROM reconciliation_runs WHERE run_id = ?",
            (reconciliation_run_id,),
        ).fetchone()
        if run is None:
            raise StateDatabaseError(
                f"unknown reconciliation run: {reconciliation_run_id}"
            )
        if run["mode"] != ReconciliationMode.APPLY.value:
            raise StateDatabaseError("repair candidates require an apply-mode run")
        if run["status"] != ReconciliationRunStatus.RUNNING.value:
            raise StateDatabaseError(
                f"repair candidate run is not active: {reconciliation_run_id}"
            )
        if not (run["start_date"] <= parsed_date.isoformat() <= run["end_date"]):
            raise StateDatabaseError(
                f"repair date {market_date} is outside reconciliation run range"
            )
        return run

    @staticmethod
    def _require_complete_repair_identity(
        *,
        prior_checksum: str | None,
        prior_row_count: int | None,
        candidate_checksum: str,
        candidate_row_count: int,
    ) -> None:
        if (prior_checksum is None) != (prior_row_count is None):
            raise StateDatabaseError(
                "historical repair identity requires both checksum and row count"
            )
        for label, checksum in (
            ("prior", prior_checksum),
            ("candidate", candidate_checksum),
        ):
            if checksum is not None and re.fullmatch(r"[0-9a-f]{64}", checksum) is None:
                raise StateDatabaseError(f"invalid {label} repair checksum")
        if prior_row_count is not None and prior_row_count <= 0:
            raise StateDatabaseError("prior repair row count must be positive")
        if candidate_row_count <= 0:
            raise StateDatabaseError("candidate repair row count must be positive")

    @staticmethod
    def _validate_repair_authorization(
        state_row: sqlite3.Row | None,
        *,
        market_date: str,
        prior_checksum: str | None,
        prior_row_count: int | None,
    ) -> None:
        if state_row is None:
            raise StateDatabaseError(
                f"repair candidate has no persistent attempt state for {market_date}"
            )
        current_status = PersistentSyncStatus(state_row["status"])
        if prior_checksum is not None:
            trusted = (
                state_row["last_verified_at"] is not None
                and state_row["csv_checksum_sha256"] is not None
                and state_row["valid_row_count"] > 0
            )
            if not trusted:
                raise StateDatabaseError(
                    f"historical repair identity is not trusted for {market_date}"
                )
            if (
                state_row["csv_checksum_sha256"] != prior_checksum
                or state_row["valid_row_count"] != prior_row_count
            ):
                raise StateDatabaseError(
                    f"historical repair identity changed for {market_date}"
                )
            if current_status not in {
                PersistentSyncStatus.FILE_MISSING,
                *VERIFIED_STATUSES,
            }:
                raise StateDatabaseError(
                    f"status {current_status.value} no longer authorizes a "
                    f"historical repair for {market_date}"
                )
            return

        if (
            state_row["last_verified_at"] is not None
            or state_row["csv_checksum_sha256"] is not None
            or current_status
            in {
                *VERIFIED_STATUSES,
                PersistentSyncStatus.CONFIRMED_NON_TRADING,
                PersistentSyncStatus.FILE_MISSING,
                PersistentSyncStatus.FILE_CORRUPT,
                PersistentSyncStatus.FILE_CONFLICT,
            }
        ):
            raise StateDatabaseError(
                f"new-gap promotion is no longer authorized for {market_date}"
            )

    @staticmethod
    def _require_pending_repair_candidate(
        connection: sqlite3.Connection,
        reconciliation_run_id: str,
        market_date: str,
    ) -> sqlite3.Row:
        candidate = connection.execute(
            """
            SELECT * FROM repair_candidates
            WHERE reconciliation_run_id = ? AND market_date = ?
            """,
            (reconciliation_run_id, market_date),
        ).fetchone()
        if candidate is None:
            raise StateDatabaseError(
                f"repair promotion lacks a durable intent for {market_date}"
            )
        if candidate["disposition"] != "PENDING_PROMOTION":
            raise StateDatabaseError(
                f"repair candidate is already terminal for {market_date}: "
                f"{candidate['disposition']}"
            )
        return candidate

    def begin_repair_candidate(
        self,
        reconciliation_run_id: str,
        market_date: str,
        staged_path: Path,
        *,
        prior_checksum: str | None,
        candidate_checksum: str,
        prior_row_count: int | None,
        candidate_row_count: int,
    ) -> None:
        """Commit an immutable promotion intent before any canonical-file mutation."""

        self._require_complete_repair_identity(
            prior_checksum=prior_checksum,
            prior_row_count=prior_row_count,
            candidate_checksum=candidate_checksum,
            candidate_row_count=candidate_row_count,
        )
        staged_path = Path(staged_path)
        inspection = inspect_canonical_csv_file(staged_path, CANONICAL_COLUMNS)
        if (
            not inspection.valid
            or inspection.checksum != candidate_checksum
            or inspection.row_count != candidate_row_count
        ):
            raise StateDatabaseError(
                f"staged repair identity changed before intent for {market_date}"
            )
        relative_path = self._relative_path(staged_path)
        now = utc_now_iso()
        expected = (
            relative_path,
            prior_checksum,
            candidate_checksum,
            prior_row_count,
            candidate_row_count,
            "VALID",
            "PENDING_PROMOTION",
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._require_running_apply_reconciliation(
                connection, reconciliation_run_id, market_date
            )
            state_row = connection.execute(
                "SELECT * FROM date_sync_state WHERE market_date = ?",
                (market_date,),
            ).fetchone()
            self._validate_repair_authorization(
                state_row,
                market_date=market_date,
                prior_checksum=prior_checksum,
                prior_row_count=prior_row_count,
            )
            existing = connection.execute(
                """
                SELECT * FROM repair_candidates
                WHERE reconciliation_run_id = ? AND market_date = ?
                """,
                (reconciliation_run_id, market_date),
            ).fetchone()
            if existing is not None:
                observed = (
                    existing["staged_relative_path"],
                    existing["prior_checksum_sha256"],
                    existing["candidate_checksum_sha256"],
                    existing["prior_row_count"],
                    existing["candidate_row_count"],
                    existing["validation_state"],
                    existing["disposition"],
                )
                if observed == expected:
                    return
                raise StateDatabaseError(
                    f"conflicting repair intent already exists for {market_date}"
                )
            connection.execute(
                """
                INSERT INTO repair_candidates (
                    reconciliation_run_id, market_date, staged_relative_path,
                    prior_checksum_sha256, candidate_checksum_sha256,
                    prior_row_count, candidate_row_count, validation_state,
                    disposition, created_at, evaluated_at, promoted_at, message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'VALID',
                          'PENDING_PROMOTION', ?, NULL, NULL, ?)
                """,
                (
                    reconciliation_run_id,
                    market_date,
                    relative_path,
                    prior_checksum,
                    candidate_checksum,
                    prior_row_count,
                    candidate_row_count,
                    now,
                    "durable intent recorded before canonical promotion",
                ),
            )

    def authorize_pending_repair_promotion(
        self,
        reconciliation_run_id: str,
        market_date: str,
        staged_path: Path,
        destination_path: Path,
    ) -> None:
        """Revalidate a pending intent and current state immediately before promotion."""

        staged_path = Path(staged_path)
        destination_path = Path(destination_path)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._require_running_apply_reconciliation(
                connection, reconciliation_run_id, market_date
            )
            candidate = self._require_pending_repair_candidate(
                connection, reconciliation_run_id, market_date
            )
            if candidate["staged_relative_path"] != self._relative_path(staged_path):
                raise StateDatabaseError(
                    f"staged repair path changed for {market_date}"
                )
            inspection = inspect_canonical_csv_file(staged_path, CANONICAL_COLUMNS)
            if (
                not inspection.valid
                or inspection.checksum != candidate["candidate_checksum_sha256"]
                or inspection.row_count != candidate["candidate_row_count"]
            ):
                raise StateDatabaseError(
                    f"staged repair identity changed for {market_date}"
                )
            if os.path.lexists(destination_path):
                raise StateDatabaseError(
                    f"canonical destination appeared before promotion for {market_date}"
                )
            state_row = connection.execute(
                "SELECT * FROM date_sync_state WHERE market_date = ?",
                (market_date,),
            ).fetchone()
            self._validate_repair_authorization(
                state_row,
                market_date=market_date,
                prior_checksum=candidate["prior_checksum_sha256"],
                prior_row_count=candidate["prior_row_count"],
            )

    @staticmethod
    def _validate_repair_decision(
        decision: DateReconciliationResult,
        *,
        run: sqlite3.Row,
        market_date: str,
        current_status: PersistentSyncStatus,
        required_status: PersistentSyncStatus,
    ) -> None:
        normalized_current = (
            PersistentSyncStatus.VERIFIED_TRADING_DATA
            if current_status is PersistentSyncStatus.ALREADY_PRESENT_VERIFIED
            else current_status
        )
        if (
            decision.market_date != market_date
            or decision.policy_version != run["policy_version"]
            or decision.previous_status is not normalized_current
            or decision.reconciled_status is not required_status
        ):
            raise StateDatabaseError(
                f"repair decision does not match current state for {market_date}"
            )

    @staticmethod
    def _terminalize_pending_candidate(
        connection: sqlite3.Connection,
        reconciliation_run_id: str,
        market_date: str,
        *,
        validation_state: str,
        disposition: str,
        message: str,
        promoted: bool,
        now: str,
    ) -> None:
        cursor = connection.execute(
            """
            UPDATE repair_candidates SET
                validation_state = ?, disposition = ?, evaluated_at = ?,
                promoted_at = ?, message = ?
            WHERE reconciliation_run_id = ? AND market_date = ?
              AND disposition = 'PENDING_PROMOTION'
            """,
            (
                validation_state,
                disposition,
                now,
                now if promoted else None,
                _clean_error(message),
                reconciliation_run_id,
                market_date,
            ),
        )
        if cursor.rowcount != 1:
            raise StateDatabaseError(
                f"repair candidate changed concurrently for {market_date}"
            )

    def finalize_promoted_repair(
        self,
        reconciliation_run_id: str,
        market_date: str,
        destination_path: Path,
        decision: DateReconciliationResult,
        *,
        message: str,
    ) -> None:
        """Atomically finalize candidate, verified state, and decision event."""

        destination_path = Path(destination_path)
        now = utc_now_iso()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            run = self._require_running_apply_reconciliation(
                connection, reconciliation_run_id, market_date
            )
            candidate = self._require_pending_repair_candidate(
                connection, reconciliation_run_id, market_date
            )
            destination = inspect_canonical_csv_file(
                destination_path, CANONICAL_COLUMNS
            )
            if (
                not destination.valid
                or destination.checksum != candidate["candidate_checksum_sha256"]
                or destination.row_count != candidate["candidate_row_count"]
            ):
                raise StateDatabaseError(
                    f"promoted canonical identity is invalid for {market_date}"
                )
            state_row = connection.execute(
                "SELECT * FROM date_sync_state WHERE market_date = ?",
                (market_date,),
            ).fetchone()
            self._validate_repair_authorization(
                state_row,
                market_date=market_date,
                prior_checksum=candidate["prior_checksum_sha256"],
                prior_row_count=candidate["prior_row_count"],
            )
            assert state_row is not None
            current_status = PersistentSyncStatus(state_row["status"])
            self._validate_repair_decision(
                decision,
                run=run,
                market_date=market_date,
                current_status=current_status,
                required_status=PersistentSyncStatus.VERIFIED_TRADING_DATA,
            )
            connection.execute(
                """
                UPDATE date_sync_state SET
                    status = ?, evidence_state = ?,
                    parsed_row_count = ?, valid_row_count = ?,
                    rejected_row_count = 0,
                    csv_checksum_sha256 = ?, csv_relative_path = ?,
                    last_success_at = COALESCE(last_success_at, ?),
                    last_verified_at = ?, last_error_type = NULL,
                    last_error_message = NULL,
                    classification_policy_version = NULL,
                    classification_basis = NULL,
                    classification_updated_at = NULL,
                    next_recheck_after = NULL, recheck_policy_version = NULL,
                    record_updated_at = ?
                WHERE market_date = ?
                """,
                (
                    PersistentSyncStatus.VERIFIED_TRADING_DATA.value,
                    SyncEvidenceState.LOCAL_CSV_SHA256_VERIFIED.value,
                    candidate["candidate_row_count"],
                    candidate["candidate_row_count"],
                    candidate["candidate_checksum_sha256"],
                    self._relative_path(destination_path),
                    now,
                    now,
                    now,
                    market_date,
                ),
            )
            _insert_reconciliation_event(
                connection, reconciliation_run_id, decision
            )
            self._terminalize_pending_candidate(
                connection,
                reconciliation_run_id,
                market_date,
                validation_state="VALID",
                disposition="PROMOTED",
                message=message,
                promoted=True,
                now=now,
            )

    def finalize_rejected_repair(
        self,
        reconciliation_run_id: str,
        market_date: str,
        destination_path: Path,
        decision: DateReconciliationResult,
        *,
        validation_state: str,
        disposition: str,
        message: str,
    ) -> None:
        """Atomically terminalize a rejected historical repair and its conflict."""

        destination_path = Path(destination_path)
        now = utc_now_iso()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            run = self._require_running_apply_reconciliation(
                connection, reconciliation_run_id, market_date
            )
            candidate = self._require_pending_repair_candidate(
                connection, reconciliation_run_id, market_date
            )
            if candidate["prior_checksum_sha256"] is None:
                raise StateDatabaseError(
                    f"new-gap repair cannot be rejected as historical for {market_date}"
                )
            if os.path.lexists(destination_path):
                raise StateDatabaseError(
                    f"rejected repair unexpectedly has a destination for {market_date}"
                )
            state_row = connection.execute(
                "SELECT * FROM date_sync_state WHERE market_date = ?",
                (market_date,),
            ).fetchone()
            self._validate_repair_authorization(
                state_row,
                market_date=market_date,
                prior_checksum=candidate["prior_checksum_sha256"],
                prior_row_count=candidate["prior_row_count"],
            )
            assert state_row is not None
            current_status = PersistentSyncStatus(state_row["status"])
            self._validate_repair_decision(
                decision,
                run=run,
                market_date=market_date,
                current_status=current_status,
                required_status=PersistentSyncStatus.FILE_CONFLICT,
            )
            connection.execute(
                """
                UPDATE date_sync_state SET
                    status = ?, evidence_state = ?,
                    last_error_type = ?, last_error_message = ?,
                    classification_policy_version = NULL,
                    classification_basis = NULL,
                    classification_updated_at = NULL,
                    next_recheck_after = NULL, recheck_policy_version = NULL,
                    record_updated_at = ?
                WHERE market_date = ?
                """,
                (
                    PersistentSyncStatus.FILE_CONFLICT.value,
                    SyncEvidenceState.LOCAL_CHECKSUM_CONFLICT.value,
                    disposition,
                    _clean_error(message),
                    now,
                    market_date,
                ),
            )
            _insert_reconciliation_event(
                connection, reconciliation_run_id, decision
            )
            self._terminalize_pending_candidate(
                connection,
                reconciliation_run_id,
                market_date,
                validation_state=validation_state,
                disposition=disposition,
                message=message,
                promoted=False,
                now=now,
            )

    def finalize_repair_candidate(
        self,
        reconciliation_run_id: str,
        market_date: str,
        *,
        validation_state: str,
        disposition: str,
        message: str,
    ) -> None:
        """Terminalize a pending candidate when no date-state mutation is allowed."""

        if disposition in {"PENDING_PROMOTION", "PROMOTED"}:
            raise ValueError("a non-promotion terminal disposition is required")
        now = utc_now_iso()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._require_running_apply_reconciliation(
                connection, reconciliation_run_id, market_date
            )
            self._require_pending_repair_candidate(
                connection, reconciliation_run_id, market_date
            )
            self._terminalize_pending_candidate(
                connection,
                reconciliation_run_id,
                market_date,
                validation_state=validation_state,
                disposition=disposition,
                message=message,
                promoted=False,
                now=now,
            )

    def recover_pending_repair_candidates(
        self,
        start_date: str,
        end_date: str,
        output_dir: Path,
        *,
        reconciliation_run_id: str,
    ) -> tuple[tuple[str, str], ...]:
        """Safely resume or audit repair intents left by terminal/stale runs."""

        observed_at = utc_now_iso()
        with self._connect() as connection:
            self._require_running_apply_reconciliation(
                connection, reconciliation_run_id, start_date
            )
            self._require_running_apply_reconciliation(
                connection, reconciliation_run_id, end_date
            )
            rows = connection.execute(
                """
                SELECT candidate.*
                FROM repair_candidates AS candidate
                JOIN reconciliation_runs AS owner
                  ON owner.run_id = candidate.reconciliation_run_id
                WHERE candidate.disposition = 'PENDING_PROMOTION'
                  AND candidate.market_date BETWEEN ? AND ?
                  AND (
                      owner.status != ?
                      OR NOT EXISTS (
                          SELECT 1
                          FROM reconciliation_recheck_claims AS claim
                          WHERE claim.market_date = candidate.market_date
                            AND claim.reconciliation_run_id = owner.run_id
                            AND claim.expires_at > ?
                      )
                  )
                ORDER BY candidate.market_date, candidate.id
                """,
                (
                    start_date,
                    end_date,
                    ReconciliationRunStatus.RUNNING.value,
                    observed_at,
                ),
            ).fetchall()
        recovered: list[tuple[str, str]] = []
        for candidate in rows:
            market_date = candidate["market_date"]
            disposition: str = "RECOVERY_STAGED_INVALID"
            validation_state: str = "INVALID"
            message: str = "unknown recovery state"
            destination_path = Path(output_dir) / f"market_{market_date}.csv"
            staged_path = (
                self.project_root / candidate["staged_relative_path"]
            ).resolve()
            destination = inspect_canonical_csv_file(
                destination_path, CANONICAL_COLUMNS
            )
            staged = inspect_canonical_csv_file(staged_path, CANONICAL_COLUMNS)
            promoted = False
            if (
                destination.valid
                and destination.checksum == candidate["candidate_checksum_sha256"]
                and destination.row_count == candidate["candidate_row_count"]
            ):
                disposition = "RECOVERED_CANONICAL_MATCH"
                validation_state = "VALID"
                message = (
                    "canonical artifact matches a durable pending intent; "
                    "promotion origin is unknown after interruption"
                )
            elif os.path.lexists(destination_path):
                disposition = "RECOVERY_DESTINATION_CONFLICT"
                validation_state = "INVALID" if not destination.valid else "VALID"
                message = (
                    "canonical destination contradicts a durable pending repair intent"
                )
            elif (
                not staged.valid
                or staged.checksum != candidate["candidate_checksum_sha256"]
                or staged.row_count != candidate["candidate_row_count"]
            ):
                disposition = "RECOVERY_STAGED_INVALID"
                validation_state = "INVALID"
                message = "staged evidence no longer matches its durable repair intent"
            else:
                authorized = False
                try:
                    with self._connect() as connection:
                        connection.execute("BEGIN IMMEDIATE")
                        current_candidate = connection.execute(
                            """
                            SELECT * FROM repair_candidates
                            WHERE id = ? AND disposition = 'PENDING_PROMOTION'
                            """,
                            (candidate["id"],),
                        ).fetchone()
                        if current_candidate is None:
                            continue
                        current_destination = inspect_canonical_csv_file(
                            destination_path, CANONICAL_COLUMNS
                        )
                        current_staged = inspect_canonical_csv_file(
                            staged_path, CANONICAL_COLUMNS
                        )
                        if os.path.lexists(destination_path):
                            destination = current_destination
                            exact_race = (
                                current_destination.valid
                                and current_destination.checksum
                                == current_candidate["candidate_checksum_sha256"]
                                and current_destination.row_count
                                == current_candidate["candidate_row_count"]
                            )
                            disposition = (
                                "RECOVERED_CANONICAL_MATCH"
                                if exact_race
                                else "RECOVERY_DESTINATION_CONFLICT"
                            )
                            validation_state = (
                                "VALID"
                                if current_destination.valid
                                else "INVALID"
                            )
                            message = (
                                "canonical destination appeared during recovery; "
                                + (
                                    "it exactly matches the pending candidate"
                                    if exact_race
                                    else "it conflicts with the pending candidate"
                                )
                            )
                        elif (
                            not current_staged.valid
                            or current_staged.checksum
                            != current_candidate["candidate_checksum_sha256"]
                            or current_staged.row_count
                            != current_candidate["candidate_row_count"]
                        ):
                            disposition = "RECOVERY_STAGED_INVALID"
                            validation_state = "INVALID"
                            message = (
                                "staged evidence changed during repair recovery"
                            )
                        else:
                            state_row = connection.execute(
                                "SELECT * FROM date_sync_state WHERE market_date = ?",
                                (market_date,),
                            ).fetchone()
                            self._validate_repair_authorization(
                                state_row,
                                market_date=market_date,
                                prior_checksum=current_candidate[
                                    "prior_checksum_sha256"
                                ],
                                prior_row_count=current_candidate[
                                    "prior_row_count"
                                ],
                            )
                            authorized = True
                except StateDatabaseError as exc:
                    disposition = "RECOVERY_AUTHORIZATION_CONFLICT"
                    validation_state = "VALID"
                    message = str(exc)
                if authorized:
                    promotion = promote_staged_csv_if_safe(
                        staged_path,
                        destination_path,
                        expected_checksum=candidate["prior_checksum_sha256"],
                        expected_row_count=candidate["prior_row_count"],
                        allow_new=candidate["prior_checksum_sha256"] is None,
                        columns=CANONICAL_COLUMNS,
                    )
                    if promotion.status is StagedPromotionStatus.PROMOTED:
                        disposition = "RECOVERY_PROMOTED"
                        validation_state = "VALID"
                        message = promotion.message
                        promoted = True
                    elif promotion.status is (
                        StagedPromotionStatus.DESTINATION_ALREADY_EXISTS
                    ):
                        raced_destination = inspect_canonical_csv_file(
                            destination_path, CANONICAL_COLUMNS
                        )
                        destination = raced_destination
                        exact_race = (
                            raced_destination.valid
                            and raced_destination.checksum
                            == candidate["candidate_checksum_sha256"]
                            and raced_destination.row_count
                            == candidate["candidate_row_count"]
                        )
                        disposition = (
                            "RECOVERED_CANONICAL_MATCH"
                            if exact_race
                            else "RECOVERY_DESTINATION_CONFLICT"
                        )
                        validation_state = (
                            "VALID"
                            if raced_destination.valid
                            else "INVALID"
                        )
                        message = (
                            "canonical destination appeared during recovery; "
                            + (
                                "it exactly matches the pending candidate"
                                if exact_race
                                else "it conflicts with the pending candidate"
                            )
                        )
                    else:
                        disposition = f"RECOVERY_{promotion.status.value}"
                        validation_state = (
                            "INVALID"
                            if promotion.status
                            is StagedPromotionStatus.STAGED_FILE_INVALID
                            else "VALID"
                        )
                        message = promotion.message

            now = utc_now_iso()
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                current_candidate = connection.execute(
                    """
                    SELECT * FROM repair_candidates
                    WHERE id = ? AND disposition = 'PENDING_PROMOTION'
                    """,
                    (candidate["id"],),
                ).fetchone()
                if current_candidate is None:
                    continue
                if disposition in {
                    "RECOVERED_CANONICAL_MATCH",
                    "RECOVERY_PROMOTED",
                }:
                    final_destination = inspect_canonical_csv_file(
                        destination_path, CANONICAL_COLUMNS
                    )
                    final_identity_matches = (
                        final_destination.valid
                        and final_destination.checksum
                        == current_candidate["candidate_checksum_sha256"]
                        and final_destination.row_count
                        == current_candidate["candidate_row_count"]
                    )
                    if not final_identity_matches:
                        destination = final_destination
                        disposition = "RECOVERY_DESTINATION_CONFLICT"
                        validation_state = (
                            "VALID" if final_destination.valid else "INVALID"
                        )
                        message = (
                            "canonical destination changed before recovery "
                            "audit finalization"
                        )
                if disposition in {
                    "RECOVERY_DESTINATION_CONFLICT",
                    "RECOVERY_AUTHORIZATION_CONFLICT",
                    "RECOVERY_HISTORICAL_MISMATCH",
                    "RECOVERY_POLICY_REJECTED",
                }:
                    recovery_run = self._require_running_apply_reconciliation(
                        connection, reconciliation_run_id, market_date
                    )
                    state_row = connection.execute(
                        "SELECT * FROM date_sync_state WHERE market_date = ?",
                        (market_date,),
                    ).fetchone()
                    if state_row is None:
                        raise StateDatabaseError(
                            f"repair recovery lacks state for {market_date}"
                        )
                    previous_status = PersistentSyncStatus(state_row["status"])
                    if previous_status is (
                        PersistentSyncStatus.ALREADY_PRESENT_VERIFIED
                    ):
                        previous_status = (
                            PersistentSyncStatus.VERIFIED_TRADING_DATA
                        )
                    connection.execute(
                        """
                        UPDATE date_sync_state SET
                            status = ?, evidence_state = ?,
                            last_error_type = ?, last_error_message = ?,
                            classification_policy_version = NULL,
                            classification_basis = NULL,
                            classification_updated_at = NULL,
                            next_recheck_after = NULL,
                            recheck_policy_version = NULL,
                            record_updated_at = ?
                        WHERE market_date = ?
                        """,
                        (
                            PersistentSyncStatus.FILE_CONFLICT.value,
                            SyncEvidenceState.LOCAL_CHECKSUM_CONFLICT.value,
                            disposition,
                            _clean_error(message),
                            now,
                            market_date,
                        ),
                    )
                    evidence_summary = json.dumps(
                        {
                            "candidate_checksum_sha256": current_candidate[
                                "candidate_checksum_sha256"
                            ],
                            "destination_checksum_sha256": destination.checksum,
                            "prior_checksum_sha256": current_candidate[
                                "prior_checksum_sha256"
                            ],
                            "recovery_disposition": disposition,
                            "staged_relative_path": current_candidate[
                                "staged_relative_path"
                            ],
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    connection.execute(
                        """
                        INSERT INTO reconciliation_events (
                            run_id, market_date, previous_status, new_status,
                            action, policy_version, evidence_classification,
                            evidence_summary, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(
                            run_id, market_date, action, new_status
                        ) DO NOTHING
                        """,
                        (
                            reconciliation_run_id,
                            market_date,
                            previous_status.value,
                            PersistentSyncStatus.FILE_CONFLICT.value,
                            "INVESTIGATE_CONFLICT",
                            recovery_run["policy_version"],
                            disposition,
                            evidence_summary,
                            now,
                        ),
                    )
                cursor = connection.execute(
                    """
                    UPDATE repair_candidates SET
                        validation_state = ?, disposition = ?, evaluated_at = ?,
                        promoted_at = ?, message = ?
                    WHERE id = ? AND disposition = 'PENDING_PROMOTION'
                    """,
                    (
                        validation_state,
                        disposition,
                        now,
                        now if promoted else None,
                        _clean_error(message),
                        candidate["id"],
                    ),
                )
                if cursor.rowcount == 1:
                    recovered.append((market_date, disposition))
        return tuple(recovered)

    def record_repair_candidate(
        self,
        reconciliation_run_id: str,
        market_date: str,
        staged_path: Path,
        *,
        prior_checksum: str | None,
        candidate_checksum: str | None,
        prior_row_count: int | None,
        candidate_row_count: int | None,
        validation_state: str,
        disposition: str,
        message: str | None,
        promoted: bool = False,
    ) -> None:
        """Record an immutable terminal candidate when promotion was never possible."""

        if promoted:
            raise StateDatabaseError(
                "promoted candidates require atomic finalize_promoted_repair"
            )
        if disposition == "PENDING_PROMOTION":
            raise StateDatabaseError(
                "pending candidates require begin_repair_candidate"
            )
        now = utc_now_iso()
        relative_path = self._relative_path(staged_path)
        cleaned_message = _clean_error(message)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._require_running_apply_reconciliation(
                connection, reconciliation_run_id, market_date
            )
            existing = connection.execute(
                """
                SELECT * FROM repair_candidates
                WHERE reconciliation_run_id = ? AND market_date = ?
                """,
                (reconciliation_run_id, market_date),
            ).fetchone()
            if existing is not None:
                exact = (
                    existing["staged_relative_path"] == relative_path
                    and existing["prior_checksum_sha256"] == prior_checksum
                    and existing["candidate_checksum_sha256"] == candidate_checksum
                    and existing["prior_row_count"] == prior_row_count
                    and existing["candidate_row_count"] == candidate_row_count
                    and existing["validation_state"] == validation_state
                    and existing["disposition"] == disposition
                    and existing["message"] == cleaned_message
                    and existing["promoted_at"] is None
                )
                if exact:
                    return
                raise StateDatabaseError(
                    f"terminal repair candidate is immutable for {market_date}"
                )
            connection.execute(
                """
                INSERT INTO repair_candidates (
                    reconciliation_run_id, market_date, staged_relative_path,
                    prior_checksum_sha256, candidate_checksum_sha256,
                    prior_row_count, candidate_row_count, validation_state,
                    disposition, created_at, evaluated_at, promoted_at, message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    reconciliation_run_id,
                    market_date,
                    relative_path,
                    prior_checksum,
                    candidate_checksum,
                    prior_row_count,
                    candidate_row_count,
                    validation_state,
                    disposition,
                    now,
                    now,
                    None,
                    cleaned_message,
                ),
            )

    @staticmethod
    def _parquet_export_from_row(row: sqlite3.Row) -> ParquetExportRecord:
        return ParquetExportRecord(
            market_date=row["market_date"],
            status=ParquetExportStatus(row["status"]),
            schema_version=row["schema_version"],
            source_csv_checksum_sha256=row["source_csv_checksum_sha256"],
            source_row_count=row["source_row_count"],
            parquet_relative_path=row["parquet_relative_path"],
            parquet_checksum_sha256=row["parquet_checksum_sha256"],
            parquet_row_count=row["parquet_row_count"],
            exporter_version=row["exporter_version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            verified_at=row["verified_at"],
            last_error=row["last_error"],
        )

    def get_parquet_export(
        self, market_date: str | date
    ) -> ParquetExportRecord | None:
        date_text = (
            market_date.isoformat()
            if isinstance(market_date, date)
            else market_date
        )
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM parquet_exports WHERE market_date = ?",
                (date_text,),
            ).fetchone()
        return None if row is None else self._parquet_export_from_row(row)

    def get_parquet_exports_for_range(
        self, start_date: str | date, end_date: str | date
    ) -> tuple[ParquetExportRecord, ...]:
        start_text = (
            start_date.isoformat()
            if isinstance(start_date, date)
            else start_date
        )
        end_text = (
            end_date.isoformat()
            if isinstance(end_date, date)
            else end_date
        )
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM parquet_exports
                WHERE market_date BETWEEN ? AND ?
                ORDER BY market_date ASC
                """,
                (start_text, end_text),
            ).fetchall()
        return tuple(self._parquet_export_from_row(row) for row in rows)

    def upsert_parquet_export(
        self,
        market_date: str | date,
        *,
        status: ParquetExportStatus | str,
        schema_version: str,
        source_csv_checksum_sha256: str,
        source_row_count: int,
        parquet_path: Path | str | None = None,
        parquet_checksum_sha256: str | None = None,
        parquet_row_count: int | None = None,
        exporter_version: str = __version__,
        verified_at: str | None = None,
        last_error: str | None = None,
    ) -> ParquetExportRecord:
        date_text = (
            market_date.isoformat()
            if isinstance(market_date, date)
            else market_date
        )
        try:
            status_enum = (
                status
                if isinstance(status, ParquetExportStatus)
                else ParquetExportStatus(status)
            )
        except ValueError as exc:
            raise StateDatabaseError(
                f"invalid parquet export status: {status!r}"
            ) from exc

        relative_path = (
            self._relative_path(Path(parquet_path))
            if parquet_path is not None
            else None
        )
        cleaned_error = _clean_error(last_error)

        if status_enum is ParquetExportStatus.CURRENT:
            if relative_path is None:
                raise StateDatabaseError(
                    f"CURRENT export state for {date_text} requires parquet_path"
                )
            if parquet_checksum_sha256 is None:
                raise StateDatabaseError(
                    f"CURRENT export state for {date_text} requires parquet_checksum_sha256"
                )
            if parquet_row_count is None:
                raise StateDatabaseError(
                    f"CURRENT export state for {date_text} requires parquet_row_count"
                )
            if parquet_row_count != source_row_count:
                raise StateDatabaseError(
                    f"CURRENT export state row count mismatch for {date_text}: "
                    f"parquet_row_count ({parquet_row_count}) != source_row_count ({source_row_count})"
                )
            if verified_at is None:
                raise StateDatabaseError(
                    f"CURRENT export state for {date_text} requires verified_at timestamp"
                )
        elif status_enum is ParquetExportStatus.MISSING:
            if (
                relative_path is not None
                or parquet_checksum_sha256 is not None
                or parquet_row_count is not None
            ):
                raise StateDatabaseError(
                    f"MISSING export state for {date_text} cannot have parquet artifact metadata"
                )
            if verified_at is not None:
                raise StateDatabaseError(
                    f"MISSING export state for {date_text} cannot have verified_at timestamp"
                )
        elif status_enum is ParquetExportStatus.STALE:
            if verified_at is not None:
                raise StateDatabaseError(
                    f"STALE export state for {date_text} cannot have verified_at timestamp"
                )
        elif status_enum is ParquetExportStatus.CORRUPT:
            if verified_at is not None:
                raise StateDatabaseError(
                    f"CORRUPT export state for {date_text} cannot have verified_at timestamp"
                )
        elif status_enum is ParquetExportStatus.FAILED:
            if verified_at is not None:
                raise StateDatabaseError(
                    f"FAILED export state for {date_text} cannot have verified_at timestamp"
                )
            if not cleaned_error:
                raise StateDatabaseError(
                    f"FAILED export state for {date_text} requires last_error"
                )

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            date_row = connection.execute(
                """
                SELECT status, csv_checksum_sha256, valid_row_count
                FROM date_sync_state WHERE market_date = ?
                """,
                (date_text,),
            ).fetchone()

            if date_row is None:
                raise StateDatabaseError(
                    f"cannot export untracked date: {date_text}"
                )

            if date_row["status"] != PersistentSyncStatus.VERIFIED_TRADING_DATA.value:
                raise StateDatabaseError(
                    f"cannot export date {date_text} with status {date_row['status']}; "
                    "must be VERIFIED_TRADING_DATA"
                )

            if source_csv_checksum_sha256 != date_row["csv_checksum_sha256"]:
                raise StateDatabaseError(
                    f"source CSV checksum mismatch for {date_text}: "
                    f"expected {date_row['csv_checksum_sha256']}, got {source_csv_checksum_sha256}"
                )

            if source_row_count != date_row["valid_row_count"]:
                raise StateDatabaseError(
                    f"source row count mismatch for {date_text}: "
                    f"expected {date_row['valid_row_count']}, got {source_row_count}"
                )

            existing = connection.execute(
                "SELECT created_at FROM parquet_exports WHERE market_date = ?",
                (date_text,),
            ).fetchone()

            now = utc_now_iso()
            created_at = existing["created_at"] if existing is not None else now

            connection.execute(
                """
                INSERT INTO parquet_exports (
                    market_date, status, schema_version, source_csv_checksum_sha256,
                    source_row_count, parquet_relative_path, parquet_checksum_sha256,
                    parquet_row_count, exporter_version, created_at, updated_at,
                    verified_at, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_date) DO UPDATE SET
                    status = excluded.status,
                    schema_version = excluded.schema_version,
                    source_csv_checksum_sha256 = excluded.source_csv_checksum_sha256,
                    source_row_count = excluded.source_row_count,
                    parquet_relative_path = excluded.parquet_relative_path,
                    parquet_checksum_sha256 = excluded.parquet_checksum_sha256,
                    parquet_row_count = excluded.parquet_row_count,
                    exporter_version = excluded.exporter_version,
                    updated_at = excluded.updated_at,
                    verified_at = excluded.verified_at,
                    last_error = excluded.last_error
                """,
                (
                    date_text,
                    status_enum.value,
                    schema_version,
                    source_csv_checksum_sha256,
                    source_row_count,
                    relative_path,
                    parquet_checksum_sha256,
                    parquet_row_count,
                    exporter_version,
                    created_at,
                    now,
                    verified_at,
                    cleaned_error,
                ),
            )
            row = connection.execute(
                "SELECT * FROM parquet_exports WHERE market_date = ?",
                (date_text,),
            ).fetchone()
            connection.commit()
            return self._parquet_export_from_row(row)

    def get_activity_logs(
        self,
        limit: int = 300,
        activity_type_filter: str | None = None,
        status_filter: str | None = None,
    ) -> tuple[LogActivityItem, ...]:
        """Query unified read-only operational activity entries across state tables."""

        items: list[LogActivityItem] = []
        with self._connect() as connection:
            cursor = connection.cursor()

            # 1. Sync Runs
            if not activity_type_filter or activity_type_filter.upper() in ("SYNC_RUN", "ALL"):
                cursor.execute(
                    """
                    SELECT run_id, command_type, start_date, end_date, started_at, finished_at, status, requested_date_count, total_valid_rows
                    FROM sync_runs
                    ORDER BY started_at DESC LIMIT ?
                    """,
                    (limit,),
                )
                for row in cursor.fetchall():
                    ts = row[4] or row[5] or ""
                    date_range = f"{row[2]} → {row[3]}" if row[2] and row[3] else (row[2] or row[3] or "—")
                    metrics = f"Dates: {row[7]}, Valid Rows: {row[8]:,}" if row[8] is not None else f"Dates: {row[7]}"
                    items.append(
                        LogActivityItem(
                            timestamp=ts,
                            activity_type="SYNC_RUN",
                            reference_id=row[0],
                            market_date_or_range=date_range,
                            status=row[6] or "UNKNOWN",
                            metrics_summary=metrics,
                            details=f"Command: {row[1]} | Status: {row[6]}",
                        )
                    )

            # 2. Reconciliation Runs
            if not activity_type_filter or activity_type_filter.upper() in ("RECONCILIATION", "ALL"):
                cursor.execute(
                    """
                    SELECT run_id, mode, start_date, end_date, started_at, finished_at, complete, requested_date_count, verified_count, status_transition_count
                    FROM reconciliation_runs
                    ORDER BY started_at DESC LIMIT ?
                    """,
                    (limit,),
                )
                for row in cursor.fetchall():
                    ts = row[4] or row[5] or ""
                    date_range = f"{row[2]} → {row[3]}" if row[2] and row[3] else (row[2] or row[3] or "—")
                    metrics = f"Dates: {row[7]}, Verified: {row[8]}, Transitions: {row[9]}"
                    items.append(
                        LogActivityItem(
                            timestamp=ts,
                            activity_type="RECONCILIATION",
                            reference_id=row[0],
                            market_date_or_range=date_range,
                            status=row[1] or "UNKNOWN",
                            metrics_summary=metrics,
                            details=f"Mode: {row[1]} | Complete: {bool(row[6])} | Policy: v1",
                        )
                    )

            # 3. Download Attempts
            if not activity_type_filter or activity_type_filter.upper() in ("DOWNLOAD_ATTEMPT", "ALL"):
                cursor.execute(
                    """
                    SELECT run_id, market_date, attempt_number, started_at, http_status, final_status, response_bytes, response_classification, error_message
                    FROM download_attempts
                    ORDER BY started_at DESC LIMIT ?
                    """,
                    (limit,),
                )
                for row in cursor.fetchall():
                    metrics = f"Attempt #{row[2]}, HTTP {row[4] or '—'}, Bytes: {row[6]:,}" if row[6] is not None else f"Attempt #{row[2]}"
                    items.append(
                        LogActivityItem(
                            timestamp=row[3] or "",
                            activity_type="DOWNLOAD_ATTEMPT",
                            reference_id=row[0],
                            market_date_or_range=row[1],
                            status=row[5] or "UNKNOWN",
                            metrics_summary=metrics,
                            details=f"HTTP: {row[4]} | Class: {row[7] or '—'} | Error: {row[8] or 'None'}",
                        )
                    )

            # 4. Parquet Exports
            if not activity_type_filter or activity_type_filter.upper() in ("PARQUET_EXPORT", "ALL"):
                cursor.execute(
                    """
                    SELECT market_date, status, schema_version, source_row_count, parquet_row_count, updated_at, last_error
                    FROM parquet_exports
                    ORDER BY updated_at DESC LIMIT ?
                    """,
                    (limit,),
                )
                for row in cursor.fetchall():
                    metrics = f"Source Rows: {row[3]:,}, Parquet Rows: {row[4] if row[4] is not None else '—'}"
                    items.append(
                        LogActivityItem(
                            timestamp=row[5] or "",
                            activity_type="PARQUET_EXPORT",
                            reference_id=row[0],
                            market_date_or_range=row[0],
                            status=row[1] or "UNKNOWN",
                            metrics_summary=metrics,
                            details=f"Schema: {row[2]} | Status: {row[1]} | Error: {row[6] or 'None'}",
                        )
                    )

        if status_filter:
            sf_upper = status_filter.upper()
            items = [i for i in items if sf_upper in i.status.upper()]

        items.sort(key=lambda x: x.timestamp, reverse=True)
        return tuple(items[:limit])


class AsyncStateRepository:
    """Async façade with one serialized writer and thread-local connections."""

    def __init__(self, repository: StateRepository) -> None:
        self.repository = repository
        self._write_lock = asyncio.Lock()

    async def run_serialized(self, operation, /, *args, **kwargs):
        """Run one composite operation and drain it before propagating cancellation.

        ``asyncio.to_thread`` cannot stop the underlying thread once it has
        started.  Letting cancellation escape immediately would allow a state
        write (or a staged-file promotion) to commit after its parent run had
        already been marked interrupted and its claims released.  Shield the
        worker, keep the single-writer lock until it is done, and only then
        deliver cancellation to the caller.
        """

        async with self._write_lock:
            worker = asyncio.create_task(
                asyncio.to_thread(operation, *args, **kwargs)
            )
            cancellation: asyncio.CancelledError | None = None
            while not worker.done():
                try:
                    await asyncio.shield(worker)
                except asyncio.CancelledError as exc:
                    if cancellation is None:
                        cancellation = exc

            try:
                result = worker.result()
            except BaseException as exc:
                if cancellation is not None:
                    raise exc from cancellation
                raise
            if cancellation is not None:
                raise cancellation
            return result

    async def prepare_fetch(
        self,
        market_date: date,
        output_dir: Path,
        columns: tuple[str, ...] = CANONICAL_COLUMNS,
    ) -> DownloadResult | None:
        return await self.run_serialized(
            self.repository.prepare_fetch,
            market_date,
            output_dir,
            columns,
        )

    async def record_attempt(
        self, run_id: str, event: DownloadAttemptEvent
    ) -> None:
        await self.run_serialized(self.repository.record_attempt, run_id, event)

    async def record_staged_attempt(
        self, run_id: str, event: DownloadAttemptEvent
    ) -> None:
        await self.run_serialized(
            self.repository.record_staged_attempt, run_id, event
        )

    async def record_download_result(
        self, run_id: str, result: DownloadResult
    ) -> None:
        await self.run_serialized(
            self.repository.record_download_result, run_id, result
        )

    async def record_staged_download_result(
        self, run_id: str, result: DownloadResult
    ) -> None:
        await self.run_serialized(
            self.repository.record_staged_download_result, run_id, result
        )

    async def index_local_file(self, path: Path) -> PersistentSyncStatus:
        return await self.run_serialized(self.repository.index_local_file, path)

    async def get_dashboard_summary(self) -> DashboardSummary:
        return await self.run_serialized(self.repository.get_dashboard_summary)

    async def get_activity_logs(
        self,
        limit: int = 300,
        activity_type_filter: str | None = None,
        status_filter: str | None = None,
    ) -> tuple[LogActivityItem, ...]:
        return await self.run_serialized(
            self.repository.get_activity_logs,
            limit=limit,
            activity_type_filter=activity_type_filter,
            status_filter=status_filter,
        )

    async def get_parquet_export(
        self, market_date: str | date
    ) -> ParquetExportRecord | None:
        return await self.run_serialized(
            self.repository.get_parquet_export, market_date
        )

    async def get_parquet_exports_for_range(
        self, start_date: str | date, end_date: str | date
    ) -> tuple[ParquetExportRecord, ...]:
        return await self.run_serialized(
            self.repository.get_parquet_exports_for_range,
            start_date,
            end_date,
        )

    async def upsert_parquet_export(
        self,
        market_date: str | date,
        *,
        status: ParquetExportStatus | str,
        schema_version: str,
        source_csv_checksum_sha256: str,
        source_row_count: int,
        parquet_path: Path | str | None = None,
        parquet_checksum_sha256: str | None = None,
        parquet_row_count: int | None = None,
        exporter_version: str = __version__,
        verified_at: str | None = None,
        last_error: str | None = None,
    ) -> ParquetExportRecord:
        return await self.run_serialized(
            self.repository.upsert_parquet_export,
            market_date,
            status=status,
            schema_version=schema_version,
            source_csv_checksum_sha256=source_csv_checksum_sha256,
            source_row_count=source_row_count,
            parquet_path=parquet_path,
            parquet_checksum_sha256=parquet_checksum_sha256,
            parquet_row_count=parquet_row_count,
            exporter_version=exporter_version,
            verified_at=verified_at,
            last_error=last_error,
        )
