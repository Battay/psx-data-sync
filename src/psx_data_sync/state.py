"""Domain types shared by the D1 and D2 download pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from pathlib import Path


RECONCILIATION_POLICY_VERSION = "psx_reconciliation_policy_v1"
WEEKEND_EMPTY_CLASSIFICATION_BASIS = (
    "OBSERVED_EMPTY_PLUS_WEEKEND_CALENDAR"
)


class ContentClassification(StrEnum):
    EQUITY_ROWS = "EQUITY_ROWS"
    EMPTY_MARKET_RESPONSE = "EMPTY_MARKET_RESPONSE"
    MALFORMED_HTML = "MALFORMED_HTML"
    NON_HTML = "NON_HTML"


class DownloadStatus(StrEnum):
    TRADING_DATA = "TRADING_DATA"
    EMPTY_MARKET_RESPONSE = "EMPTY_MARKET_RESPONSE"
    NON_TRADING_OR_EMPTY = "NON_TRADING_OR_EMPTY"
    CONFIRMED_NON_TRADING = "CONFIRMED_NON_TRADING"
    TEMPORARY_FAILURE = "TEMPORARY_FAILURE"
    HTTP_FAILURE = "HTTP_FAILURE"
    PARSE_FAILURE = "PARSE_FAILURE"
    VALIDATION_FAILURE = "VALIDATION_FAILURE"
    ALREADY_PRESENT = "ALREADY_PRESENT"
    REPAIR_REQUIRED = "REPAIR_REQUIRED"
    EXISTING_FILE_INVALID = "EXISTING_FILE_INVALID"
    FILE_CONFLICT = "FILE_CONFLICT"
    SAVE_FAILURE = "SAVE_FAILURE"
    INVALID_DATE = "INVALID_DATE"


class ClientFailureKind(StrEnum):
    TIMEOUT = "TIMEOUT"
    CONNECTION = "CONNECTION"
    HTTP = "HTTP"
    EMPTY_BODY = "EMPTY_BODY"


class SaveStatus(StrEnum):
    CREATED = "CREATED"
    ALREADY_PRESENT = "ALREADY_PRESENT"
    EXISTING_FILE_INVALID = "EXISTING_FILE_INVALID"
    CONFLICT = "CONFLICT"


class PersistentSyncStatus(StrEnum):
    NEVER_ATTEMPTED = "NEVER_ATTEMPTED"
    VERIFIED_TRADING_DATA = "VERIFIED_TRADING_DATA"
    ALREADY_PRESENT_VERIFIED = "ALREADY_PRESENT_VERIFIED"
    EMPTY_UNRESOLVED = "EMPTY_UNRESOLVED"
    TEMPORARY_FAILURE = "TEMPORARY_FAILURE"
    HTTP_FAILURE = "HTTP_FAILURE"
    PARSE_FAILURE = "PARSE_FAILURE"
    VALIDATION_FAILURE = "VALIDATION_FAILURE"
    FILE_CONFLICT = "FILE_CONFLICT"
    FILE_CORRUPT = "FILE_CORRUPT"
    FILE_MISSING = "FILE_MISSING"
    CONFIRMED_NON_TRADING = "CONFIRMED_NON_TRADING"


class SyncRunStatus(StrEnum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_UNRESOLVED = "COMPLETED_WITH_UNRESOLVED"
    COMPLETED_WITH_FAILURES = "COMPLETED_WITH_FAILURES"
    INTERRUPTED = "INTERRUPTED"


class SyncEvidenceState(StrEnum):
    NONE = "NONE"
    LOCAL_CSV_SHA256_VERIFIED = "LOCAL_CSV_SHA256_VERIFIED"
    NETWORK_VALIDATED_CSV = "NETWORK_VALIDATED_CSV"
    NETWORK_OBSERVATION = "NETWORK_OBSERVATION"
    LOCAL_FILE_MISSING = "LOCAL_FILE_MISSING"
    LOCAL_FILE_CORRUPT = "LOCAL_FILE_CORRUPT"
    LOCAL_CHECKSUM_CONFLICT = "LOCAL_CHECKSUM_CONFLICT"
    REPEATED_EMPTY_WITH_WEEKEND_CALENDAR = (
        "REPEATED_EMPTY_WITH_WEEKEND_CALENDAR"
    )


class ReconciliationAction(StrEnum):
    NO_ACTION = "NO_ACTION"
    NETWORK_RECHECK = "NETWORK_RECHECK"
    LOCAL_REINDEX = "LOCAL_REINDEX"
    REPAIR_MISSING_FILE = "REPAIR_MISSING_FILE"
    INVESTIGATE_CORRUPT_FILE = "INVESTIGATE_CORRUPT_FILE"
    INVESTIGATE_CONFLICT = "INVESTIGATE_CONFLICT"
    CONFIRM_NON_TRADING = "CONFIRM_NON_TRADING"
    MANUAL_REVIEW = "MANUAL_REVIEW"


class FileHealthState(StrEnum):
    HEALTHY = "HEALTHY"
    ABSENT = "ABSENT"
    MISSING = "MISSING"
    CORRUPT = "CORRUPT"
    CONFLICT = "CONFLICT"
    UNTRACKED_VALID = "UNTRACKED_VALID"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class ChecksumState(StrEnum):
    MATCH = "MATCH"
    MISSING = "MISSING"
    MISMATCH = "MISMATCH"
    UNTRACKED = "UNTRACKED"
    UNREADABLE = "UNREADABLE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class ReconciliationMode(StrEnum):
    DRY_RUN = "DRY_RUN"
    APPLY = "APPLY"


class ReconciliationRunStatus(StrEnum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    INTERRUPTED = "INTERRUPTED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class FetchResponse:
    status_code: int
    content: bytes


@dataclass(frozen=True, slots=True)
class DownloadAttemptEvent:
    requested_date: str
    attempt_number: int
    started_at: str
    finished_at: str
    duration_ms: float
    http_status: int | None
    response_bytes: int
    response_classification: str | None
    final_status: DownloadStatus
    retryable: bool
    error_type: str | None = None
    error_message: str | None = None
    parsed_row_count: int = 0
    valid_row_count: int = 0
    rejected_row_count: int = 0
    checksum: str | None = None
    saved_path: Path | None = None
    worker_identifier: str | None = None


@dataclass(frozen=True, slots=True)
class ParsedEquityRow:
    row_index: int
    raw_values: tuple[str, ...]
    symbol: str
    ldcp: Decimal | None
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    change: Decimal | None
    change_percent: Decimal | None
    volume: int | None
    parse_errors: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ValidEquityRow:
    row_index: int
    symbol: str
    ldcp: Decimal
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    change: Decimal
    change_percent: Decimal
    volume: int


@dataclass(frozen=True, slots=True)
class RejectedRow:
    row_index: int
    symbol: str
    raw_values: tuple[str, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ValidationResult:
    valid_rows: tuple[ValidEquityRow, ...]
    rejected_rows: tuple[RejectedRow, ...]


@dataclass(frozen=True, slots=True)
class SaveResult:
    status: SaveStatus
    path: Path
    checksum: str | None
    message: str | None = None


@dataclass(frozen=True, slots=True)
class DownloadResult:
    requested_date: str
    status: DownloadStatus
    http_status: int | None = None
    attempts: int = 0
    response_bytes: int = 0
    cumulative_response_bytes: int = 0
    parsed_row_count: int = 0
    valid_row_count: int = 0
    rejected_row_count: int = 0
    elapsed_ms: float = 0.0
    network_ms: float = 0.0
    parse_ms: float = 0.0
    validation_ms: float = 0.0
    save_ms: float = 0.0
    saved_path: Path | None = None
    checksum: str | None = None
    rate_limit_count: int = 0
    locally_skipped: bool = False
    warnings: tuple[str, ...] = ()
    error: str | None = None

    @property
    def successful(self) -> bool:
        return self.status in {
            DownloadStatus.TRADING_DATA,
            DownloadStatus.ALREADY_PRESENT,
        }

    @property
    def transferred_response_bytes(self) -> int:
        """Include every HTTP attempt while preserving the D1 final-size field."""

        return self.cumulative_response_bytes or self.response_bytes


@dataclass(frozen=True, slots=True)
class ExistingFileInspection:
    path: Path
    exists: bool
    valid: bool
    row_count: int = 0
    checksum: str | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class RangeDownloadResult:
    start_date: str
    end_date: str
    requested_dates: tuple[str, ...]
    workers: int
    total_duration_ms: float
    results: tuple[DownloadResult, ...]
    counts_by_status: dict[DownloadStatus, int]
    total_parsed_rows: int
    total_valid_rows: int
    total_rejected_rows: int
    total_response_bytes: int
    total_retries: int
    rate_limit_occurrences: int
    network_fetched_dates: int
    locally_skipped_dates: int
    verified_successful_dates: int
    failed_dates: tuple[str, ...]
    unresolved_empty_dates: tuple[str, ...]
    average_per_date_duration_ms: float
    dates_per_second: float
    verified_dates_per_second: float
    network_dates_per_second: float
    rows_per_second: float
    warnings: tuple[str, ...] = ()
    run_id: str | None = None

    @property
    def requested_count(self) -> int:
        return len(self.requested_dates)

    @property
    def has_failures(self) -> bool:
        return bool(self.failed_dates)

    @property
    def has_unresolved_empty(self) -> bool:
        return bool(self.unresolved_empty_dates)


@dataclass(frozen=True, slots=True)
class BenchmarkMetrics:
    workers: int
    requested_dates: int
    total_duration_ms: float
    dates_per_second: float
    verified_dates_per_second: float
    network_dates_per_second: float
    rows_per_second: float
    retries: int
    failures: int
    response_bytes: int


@dataclass(frozen=True, slots=True)
class DateSyncState:
    market_date: str
    status: PersistentSyncStatus
    evidence_state: SyncEvidenceState
    attempt_count: int
    successful_attempt_count: int
    last_http_status: int | None
    last_response_bytes: int
    parsed_row_count: int
    valid_row_count: int
    rejected_row_count: int
    csv_checksum_sha256: str | None
    csv_relative_path: str | None
    first_attempt_at: str | None
    last_attempt_at: str | None
    last_success_at: str | None
    last_verified_at: str | None
    last_error_type: str | None
    last_error_message: str | None
    last_duration_ms: float | None
    source_endpoint: str
    record_created_at: str
    record_updated_at: str
    classification_policy_version: str | None = None
    classification_basis: str | None = None
    classification_updated_at: str | None = None
    next_recheck_after: str | None = None
    recheck_policy_version: str | None = None


@dataclass(frozen=True, slots=True)
class SyncRunRecord:
    run_id: str
    command_type: str
    start_date: str
    end_date: str
    requested_date_count: int
    worker_count: int
    started_at: str
    finished_at: str | None
    duration_ms: float | None
    completed_count: int
    network_fetch_count: int
    local_skip_count: int
    success_count: int
    unresolved_count: int
    failure_count: int
    total_valid_rows: int
    total_rejected_rows: int
    total_response_bytes: int
    total_attempts: int
    interrupted: bool
    status: SyncRunStatus
    application_version: str


@dataclass(frozen=True, slots=True)
class DownloadAttemptRecord:
    id: int
    run_id: str
    market_date: str
    attempt_number: int
    started_at: str
    finished_at: str
    duration_ms: float
    http_status: int | None
    response_bytes: int
    response_classification: str | None
    final_status: str
    retryable: bool
    error_type: str | None
    error_message: str | None
    parsed_row_count: int
    valid_row_count: int
    rejected_row_count: int
    checksum: str | None
    csv_relative_path: str | None
    worker_identifier: str | None
    created_at: str


@dataclass(frozen=True, slots=True)
class StateSummary:
    database_path: Path
    tracked_dates: int
    counts_by_status: dict[PersistentSyncStatus, int]
    earliest_tracked: str | None
    latest_tracked: str | None
    last_successful_sync: str | None


@dataclass(frozen=True, slots=True)
class BootstrapFileResult:
    path: Path
    market_date: str | None
    status: PersistentSyncStatus
    row_count: int
    checksum: str | None
    changed: bool
    error: str | None = None


@dataclass(frozen=True, slots=True)
class BootstrapResult:
    discovered_files: int
    indexed_files: int
    unchanged_files: int
    invalid_files: int
    files: tuple[BootstrapFileResult, ...]


@dataclass(frozen=True, slots=True)
class AttemptEvidenceRecord:
    market_date: str
    attempt_count: int
    empty_observation_count: int
    independent_empty_run_count: int
    valid_observation_count: int
    independent_valid_run_count: int
    http_statuses: tuple[int, ...]
    response_classifications: tuple[str, ...]
    first_observed_at: str | None
    last_observed_at: str | None
    empty_run_observations: tuple[tuple[str, str], ...] = ()
    latest_valid_checksum: str | None = None
    latest_valid_relative_path: str | None = None


@dataclass(frozen=True, slots=True)
class ReconciliationEvidenceSummary:
    weekday: str
    calendar_weekend: bool
    calendar_support: str | None
    persistent_evidence: str | None
    http_statuses: tuple[int, ...]
    response_classifications: tuple[str, ...]
    independent_empty_run_count: int
    independent_valid_run_count: int
    adjacent_previous_verified: bool
    adjacent_next_verified: bool
    expected_csv_path: str
    expected_checksum: str | None
    observed_checksum: str | None


@dataclass(frozen=True, slots=True)
class DateReconciliationResult:
    market_date: str
    previous_status: PersistentSyncStatus
    reconciled_status: PersistentSyncStatus
    policy_version: str
    evidence_classification: str
    action_required: ReconciliationAction
    network_recheck_required: bool
    recheck_eligible_now: bool
    local_repair_required: bool
    evidence_summary: ReconciliationEvidenceSummary
    attempt_count: int
    empty_observation_count: int
    valid_observation_count: int
    file_state: FileHealthState
    checksum_state: ChecksumState
    next_recheck_after: str | None = None
    reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    # Internal optimistic-concurrency snapshot.  Presentation layers omit
    # these fields, but apply-mode mutations must be tied to the exact state
    # row used to build the decision.
    state_snapshot_exists: bool = False
    state_record_updated_at: str | None = None

    @property
    def resolved(self) -> bool:
        return (
            self.reconciled_status
            is PersistentSyncStatus.VERIFIED_TRADING_DATA
            and self.file_state is FileHealthState.HEALTHY
            and self.checksum_state is ChecksumState.MATCH
        ) or (
            self.reconciled_status
            is PersistentSyncStatus.CONFIRMED_NON_TRADING
            and self.file_state is FileHealthState.NOT_APPLICABLE
        )

    @property
    def has_problem(self) -> bool:
        return not self.resolved or self.action_required is not ReconciliationAction.NO_ACTION


@dataclass(frozen=True, slots=True)
class ReconciliationRangeResult:
    run_id: str
    start_date: str
    end_date: str
    mode: ReconciliationMode
    policy_version: str
    requested_dates: tuple[str, ...]
    results: tuple[DateReconciliationResult, ...]
    complete: bool
    resolution_percentage: float
    counts_by_status: dict[PersistentSyncStatus, int]
    counts_by_action: dict[ReconciliationAction, int]
    verified_count: int
    confirmed_non_trading_count: int
    never_attempted_count: int
    unresolved_count: int
    failure_count: int
    file_health_issue_count: int
    network_recheck_planned_count: int
    network_recheck_count: int
    local_repair_count: int
    manual_review_count: int
    status_transition_count: int
    network_rechecked_dates: tuple[str, ...] = ()
    staged_repair_dates: tuple[str, ...] = ()
    promoted_repair_dates: tuple[str, ...] = ()
    duration_ms: float = 0.0
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ReconciliationRunRecord:
    run_id: str
    policy_version: str
    start_date: str
    end_date: str
    mode: ReconciliationMode
    requested_date_count: int
    worker_count: int
    force_recheck: bool
    max_rechecks_per_date: int
    cooldown_seconds: float
    verified_count: int
    confirmed_non_trading_count: int
    never_attempted_count: int
    unresolved_count: int
    failure_count: int
    file_health_issue_count: int
    network_recheck_planned_count: int
    network_recheck_count: int
    local_repair_count: int
    manual_review_count: int
    status_transition_count: int
    complete: bool
    linked_sync_run_id: str | None
    started_at: str
    finished_at: str | None
    duration_ms: float | None
    interrupted: bool
    status: ReconciliationRunStatus
    error_message: str | None
    application_version: str


class ParquetExportStatus(StrEnum):
    """Persistent state of one derived Parquet market-date partition."""

    CURRENT = "CURRENT"
    MISSING = "MISSING"
    STALE = "STALE"
    CORRUPT = "CORRUPT"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class ParquetExportRecord:
    """Persistent provenance for one derived Parquet partition."""

    market_date: str
    status: ParquetExportStatus
    schema_version: str
    source_csv_checksum_sha256: str
    source_row_count: int
    parquet_relative_path: str | None
    parquet_checksum_sha256: str | None
    parquet_row_count: int | None
    exporter_version: str
    created_at: str
    updated_at: str
    verified_at: str | None
    last_error: str | None
