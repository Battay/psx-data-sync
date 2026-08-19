"""Evidence-based range reconciliation and conservative safe repair.

This module owns D4 policy and orchestration.  It deliberately keeps HTTP in
``client.py``, row handling in the D1 pipeline, artifact mechanics in
``exporter.py``, and persistence in ``state_db.py``.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import Counter
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

from .client import AsyncPSXClient
from .config import Settings
from .exporter import (
    StagedPromotionStatus,
    inspect_canonical_csv_file,
    inspect_existing_canonical_file,
    promote_staged_csv_if_safe,
)
from .state import (
    AttemptEvidenceRecord,
    ChecksumState,
    DateReconciliationResult,
    DateSyncState,
    DownloadResult,
    FileHealthState,
    PersistentSyncStatus,
    ReconciliationAction,
    ReconciliationEvidenceSummary,
    ReconciliationMode,
    ReconciliationRangeResult,
    RECONCILIATION_POLICY_VERSION,
    WEEKEND_EMPTY_CLASSIFICATION_BASIS,
)
from .state_db import AsyncStateRepository, StateDatabaseError, StateRepository
from .synchronizer import (
    ConcurrentRangeDownloader,
    generate_date_range,
    validate_workers,
)


POLICY_VERSION = RECONCILIATION_POLICY_VERSION
MINIMUM_EMPTY_OBSERVATION_SEPARATION = timedelta(hours=24)
RECHECK_CLAIM_LEASE = timedelta(hours=6)

_VERIFIED_STATUSES = frozenset(
    {
        PersistentSyncStatus.VERIFIED_TRADING_DATA,
        PersistentSyncStatus.ALREADY_PRESENT_VERIFIED,
    }
)
_FAILURE_STATUSES = frozenset(
    {
        PersistentSyncStatus.TEMPORARY_FAILURE,
        PersistentSyncStatus.HTTP_FAILURE,
        PersistentSyncStatus.PARSE_FAILURE,
        PersistentSyncStatus.VALIDATION_FAILURE,
    }
)
_FORCEABLE_STATUSES = frozenset(
    {
        PersistentSyncStatus.EMPTY_UNRESOLVED,
        *_FAILURE_STATUSES,
    }
)
_FILE_ISSUE_STATUSES = frozenset(
    {
        PersistentSyncStatus.FILE_MISSING,
        PersistentSyncStatus.FILE_CORRUPT,
        PersistentSyncStatus.FILE_CONFLICT,
    }
)


def _aware_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _normalized_status(status: PersistentSyncStatus) -> PersistentSyncStatus:
    if status is PersistentSyncStatus.ALREADY_PRESENT_VERIFIED:
        return PersistentSyncStatus.VERIFIED_TRADING_DATA
    return status


def _empty_evidence_count(
    evidence: AttemptEvidenceRecord,
) -> tuple[int, tuple[str, ...]]:
    """Count distinct-run empty observations that are at least a day apart."""

    observations: list[tuple[datetime, str]] = []
    warnings: list[str] = []
    for run_id, observed_at in evidence.empty_run_observations:
        parsed = _aware_datetime(observed_at)
        if parsed is None:
            warnings.append(
                f"ignored empty observation from run {run_id}: invalid timestamp"
            )
            continue
        observations.append((parsed, run_id))
    observations.sort(key=lambda item: (item[0], item[1]))

    accepted: list[datetime] = []
    for observed_at, _ in observations:
        if not accepted or observed_at - accepted[-1] >= (
            MINIMUM_EMPTY_OBSERVATION_SEPARATION
        ):
            accepted.append(observed_at)
    return len(accepted), tuple(warnings)


def _empty_evidence(market_date: str) -> AttemptEvidenceRecord:
    return AttemptEvidenceRecord(
        market_date=market_date,
        attempt_count=0,
        empty_observation_count=0,
        independent_empty_run_count=0,
        valid_observation_count=0,
        independent_valid_run_count=0,
        http_statuses=(),
        response_classifications=(),
        first_observed_at=None,
        last_observed_at=None,
    )


def _expected_relative_path(repository: StateRepository, path: Path) -> str:
    return Path(
        os.path.relpath(path.resolve(), repository.project_root)
    ).as_posix()


def _historical_verified_identity(state: DateSyncState | None) -> bool:
    """Return whether canonical artifact identity was verified previously.

    A response containing valid equity rows is evidence that trading occurred,
    but a SAVE_FAILURE does not establish a canonical artifact identity.  That
    distinction lets a later successful recheck create the first canonical
    file while still preventing a false non-trading conclusion.
    """

    if state is None:
        return False
    return bool(
        state.last_verified_at
        or state.status in _VERIFIED_STATUSES
    )


def _cooldown(
    state: DateSyncState | None,
    evidence: AttemptEvidenceRecord,
    *,
    now: datetime,
    cooldown_seconds: float,
    force_recheck: bool,
    status: PersistentSyncStatus,
) -> tuple[bool, str | None]:
    """Return (eligible now, next timestamp) for a network action."""

    if state is not None and state.last_error_type == "CANCELLED":
        return True, None
    stored_next: datetime | None = None
    if (
        state is not None
        and state.next_recheck_after
        and state.recheck_policy_version == POLICY_VERSION
    ):
        stored_next = _aware_datetime(state.next_recheck_after)
    last_attempt = _aware_datetime(
        state.last_attempt_at if state is not None else evidence.last_observed_at
    )
    if force_recheck and status in _FORCEABLE_STATUSES:
        return True, None
    derived_next = (
        None
        if last_attempt is None or cooldown_seconds == 0
        else last_attempt + timedelta(seconds=cooldown_seconds)
    )
    candidates = tuple(
        candidate
        for candidate in (stored_next, derived_next)
        if candidate is not None
    )
    if not candidates:
        return True, None
    next_recheck = max(candidates)
    if now >= next_recheck:
        return True, None
    return False, _iso(next_recheck)


class ReconciliationPlanner:
    """Build deterministic plans from bulk state/evidence and local artifacts."""

    def __init__(
        self,
        settings: Settings,
        repository: StateRepository,
        *,
        now: datetime | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        resolved_now = now or _utc_now()
        self.now = (
            resolved_now.replace(tzinfo=timezone.utc)
            if resolved_now.tzinfo is None
            else resolved_now.astimezone(timezone.utc)
        )

    def plan(
        self,
        requested_dates: Iterable[date],
        *,
        run_id: str,
        mode: ReconciliationMode,
        force_recheck: bool = False,
        network_recheck_count: int = 0,
        network_rechecked_dates: tuple[str, ...] = (),
        staged_repair_dates: tuple[str, ...] = (),
        promoted_repair_dates: tuple[str, ...] = (),
        duration_ms: float = 0.0,
        warnings: tuple[str, ...] = (),
        planned_count_override: int | None = None,
        status_transition_count: int = 0,
    ) -> ReconciliationRangeResult:
        dates = tuple(sorted(set(requested_dates)))
        if not dates:
            raise ValueError("at least one requested date is required")
        context_dates = tuple(
            sorted(
                {
                    contextual
                    for day in dates
                    for contextual in (
                        day - timedelta(days=1),
                        day,
                        day + timedelta(days=1),
                    )
                }
            )
        )
        start_text = dates[0].isoformat()
        end_text = dates[-1].isoformat()
        states = self.repository.get_date_states_for_range(
            context_dates[0].isoformat(), context_dates[-1].isoformat()
        )
        evidence_by_date = self.repository.get_attempt_evidence_for_range(
            start_text, end_text
        )
        inspections = {
            day.isoformat(): inspect_existing_canonical_file(
                day,
                self.settings.raw_output_dir,
                self.settings.canonical_columns,
            )
            for day in context_dates
        }

        adjacent_healthy: set[str] = set()
        for day in context_dates:
            date_text = day.isoformat()
            state = states.get(date_text)
            inspection = inspections[date_text]
            if (
                state is not None
                and state.status in _VERIFIED_STATUSES
                and inspection.valid
                and (
                    state.csv_checksum_sha256 is None
                    or state.csv_checksum_sha256 == inspection.checksum
                )
            ):
                adjacent_healthy.add(date_text)

        results = tuple(
            self._plan_date(
                day,
                states.get(day.isoformat()),
                evidence_by_date.get(
                    day.isoformat(), _empty_evidence(day.isoformat())
                ),
                inspections[day.isoformat()],
                adjacent_healthy,
                force_recheck=force_recheck,
            )
            for day in dates
        )
        return _range_result(
            run_id=run_id,
            dates=dates,
            mode=mode,
            results=results,
            network_recheck_count=network_recheck_count,
            network_rechecked_dates=network_rechecked_dates,
            staged_repair_dates=staged_repair_dates,
            promoted_repair_dates=promoted_repair_dates,
            duration_ms=duration_ms,
            warnings=warnings,
            planned_count_override=planned_count_override,
            status_transition_count=status_transition_count,
        )

    def _plan_date(
        self,
        day: date,
        state: DateSyncState | None,
        evidence: AttemptEvidenceRecord,
        inspection,
        adjacent_healthy: set[str],
        *,
        force_recheck: bool,
    ) -> DateReconciliationResult:
        market_date = day.isoformat()
        previous_status = (
            PersistentSyncStatus.NEVER_ATTEMPTED
            if state is None
            else _normalized_status(state.status)
        )
        current_status = previous_status
        weekend = day.weekday() >= 5
        calendar_support = "SATURDAY_OR_SUNDAY" if weekend else None
        policy_empty_count, timestamp_warnings = _empty_evidence_count(evidence)
        warnings = list(timestamp_warnings)
        expected_path = inspection.path
        expected_relative = _expected_relative_path(self.repository, expected_path)
        historical_trading = _historical_verified_identity(state)
        has_valid_observation = evidence.valid_observation_count > 0
        expected_checksum = (
            state.csv_checksum_sha256
            if (
                state is not None
                and historical_trading
                and state.csv_checksum_sha256
            )
            else evidence.latest_valid_checksum
        )
        reasons: list[str] = []

        action = ReconciliationAction.NO_ACTION
        reconciled = current_status
        evidence_classification = "INSUFFICIENT_EVIDENCE"
        file_state = FileHealthState.ABSENT
        checksum_state = ChecksumState.NOT_APPLICABLE
        local_repair = False

        if inspection.valid:
            if (
                current_status is PersistentSyncStatus.FILE_CONFLICT
                and not (
                    historical_trading
                    and state is not None
                    and state.csv_checksum_sha256 is not None
                )
            ):
                reconciled = PersistentSyncStatus.FILE_CONFLICT
                action = ReconciliationAction.INVESTIGATE_CONFLICT
                evidence_classification = "PERSISTED_UNANCHORED_CONFLICT"
                file_state = FileHealthState.CONFLICT
                checksum_state = (
                    ChecksumState.MISMATCH
                    if expected_checksum
                    and expected_checksum != inspection.checksum
                    else ChecksumState.UNTRACKED
                )
                local_repair = True
                reasons.append(
                    "an unanchored conflict requires manual review before adopting local data"
                )
            elif expected_checksum and expected_checksum != inspection.checksum:
                reconciled = PersistentSyncStatus.FILE_CONFLICT
                action = ReconciliationAction.INVESTIGATE_CONFLICT
                evidence_classification = "LOCAL_CHECKSUM_CONFLICT"
                file_state = FileHealthState.CONFLICT
                checksum_state = ChecksumState.MISMATCH
                local_repair = True
                reasons.append(
                    "valid canonical CSV checksum contradicts historical identity"
                )
            else:
                reconciled = PersistentSyncStatus.VERIFIED_TRADING_DATA
                checksum_state = (
                    ChecksumState.MATCH
                    if expected_checksum
                    else ChecksumState.UNTRACKED
                )
                metadata_exact = bool(
                    state is not None
                    and state.status in _VERIFIED_STATUSES
                    and state.csv_checksum_sha256 == inspection.checksum
                    and state.valid_row_count == inspection.row_count
                    and state.csv_relative_path == expected_relative
                )
                if metadata_exact:
                    file_state = FileHealthState.HEALTHY
                    evidence_classification = "VALIDATED_TRADING_DATA"
                    reasons.append(
                        "canonical CSV is valid and matches persistent SHA-256"
                    )
                else:
                    file_state = FileHealthState.UNTRACKED_VALID
                    action = ReconciliationAction.LOCAL_REINDEX
                    local_repair = True
                    evidence_classification = "VALID_LOCAL_ARTIFACT"
                    reasons.append(
                        "valid canonical CSV requires metadata re-indexing"
                    )
                if current_status is PersistentSyncStatus.CONFIRMED_NON_TRADING:
                    warnings.append(
                        "valid trading data overrides the prior non-trading conclusion"
                    )
        elif inspection.exists:
            reconciled = PersistentSyncStatus.FILE_CORRUPT
            action = ReconciliationAction.INVESTIGATE_CORRUPT_FILE
            evidence_classification = "LOCAL_ARTIFACT_INVALID"
            file_state = FileHealthState.CORRUPT
            checksum_state = (
                ChecksumState.MISMATCH
                if inspection.checksum and expected_checksum
                else (
                    ChecksumState.UNTRACKED
                    if inspection.checksum
                    else ChecksumState.UNREADABLE
                )
            )
            local_repair = True
            reasons.append(
                inspection.error or "canonical CSV failed strict validation"
            )
            if historical_trading:
                reasons.append("historical trading identity remains preserved")
        elif current_status is PersistentSyncStatus.FILE_CONFLICT:
            reconciled = PersistentSyncStatus.FILE_CONFLICT
            action = ReconciliationAction.INVESTIGATE_CONFLICT
            evidence_classification = "PERSISTED_REPAIR_CONFLICT"
            file_state = FileHealthState.CONFLICT
            checksum_state = (
                ChecksumState.MISSING
                if expected_checksum
                else ChecksumState.UNTRACKED
            )
            local_repair = True
            reasons.append(
                "a prior staged/canonical contradiction remains unresolved"
            )
        elif historical_trading:
            reconciled = PersistentSyncStatus.FILE_MISSING
            action = ReconciliationAction.REPAIR_MISSING_FILE
            evidence_classification = "HISTORICAL_TRADING_ARTIFACT_MISSING"
            file_state = FileHealthState.MISSING
            checksum_state = (
                ChecksumState.MISSING
                if expected_checksum
                else ChecksumState.UNTRACKED
            )
            local_repair = True
            reasons.append(
                "trading evidence exists but the expected canonical CSV is missing"
            )
            if expected_checksum is None:
                warnings.append(
                    "historical SHA-256 is unavailable; an automatic repair cannot be promoted"
                )
        elif (
            current_status is PersistentSyncStatus.CONFIRMED_NON_TRADING
            and state is not None
            and state.classification_policy_version == POLICY_VERSION
            and state.classification_basis
            == WEEKEND_EMPTY_CLASSIFICATION_BASIS
            and weekend
            and policy_empty_count >= 2
            and not has_valid_observation
        ):
            reconciled = PersistentSyncStatus.CONFIRMED_NON_TRADING
            evidence_classification = (
                state.classification_basis
                if state is not None and state.classification_basis
                else "PERSISTED_NON_TRADING_CONCLUSION"
            )
            file_state = FileHealthState.NOT_APPLICABLE
            checksum_state = ChecksumState.NOT_APPLICABLE
            reasons.append("versioned non-trading conclusion has no contradiction")
        elif (
            current_status is PersistentSyncStatus.CONFIRMED_NON_TRADING
            and not (weekend and policy_empty_count >= 2)
        ):
            reconciled = PersistentSyncStatus.EMPTY_UNRESOLVED
            action = ReconciliationAction.MANUAL_REVIEW
            evidence_classification = "UNSUPPORTED_NON_TRADING_CONCLUSION"
            reasons.append(
                "stored non-trading conclusion cannot be reproduced under policy v1"
            )
            warnings.append(
                "the stored conclusion policy/basis or independent evidence is missing"
            )
        elif (
            weekend
            and policy_empty_count >= 2
            and not has_valid_observation
        ):
            reconciled = PersistentSyncStatus.CONFIRMED_NON_TRADING
            action = ReconciliationAction.CONFIRM_NON_TRADING
            evidence_classification = WEEKEND_EMPTY_CLASSIFICATION_BASIS
            file_state = FileHealthState.NOT_APPLICABLE
            checksum_state = ChecksumState.NOT_APPLICABLE
            reasons.extend(
                (
                    "at least two independent valid PSX empty responses were observed",
                    "Saturday/Sunday calendar context supports the conclusion",
                )
            )
        elif state is None or (
            current_status is PersistentSyncStatus.NEVER_ATTEMPTED
            and state.attempt_count == 0
        ):
            reconciled = PersistentSyncStatus.NEVER_ATTEMPTED
            action = ReconciliationAction.NETWORK_RECHECK
            evidence_classification = (
                "CALENDAR_SUPPORT_ONLY" if weekend else "NO_OBSERVATION"
            )
            reasons.append("no persistent network or artifact evidence exists")
            if weekend:
                reasons.append(
                    "weekend alone is supporting context, not a permanent conclusion"
                )
        elif current_status in _FAILURE_STATUSES:
            reconciled = current_status
            action = ReconciliationAction.NETWORK_RECHECK
            evidence_classification = "OBSERVED_RETRYABLE_FAILURE"
            reasons.append(
                f"latest persistent outcome is {current_status.value}"
            )
            if has_valid_observation:
                reasons.append(
                    "valid trading rows were observed but no canonical artifact was verified"
                )
        elif has_valid_observation:
            reconciled = PersistentSyncStatus.TEMPORARY_FAILURE
            action = ReconciliationAction.NETWORK_RECHECK
            evidence_classification = "TRADING_OBSERVED_ARTIFACT_UNVERIFIED"
            reasons.append(
                "valid trading rows were observed but no canonical artifact was verified"
            )
        elif (
            current_status is PersistentSyncStatus.EMPTY_UNRESOLVED
            or evidence.empty_observation_count > 0
        ):
            reconciled = PersistentSyncStatus.EMPTY_UNRESOLVED
            evidence_classification = "OBSERVED_EMPTY_UNRESOLVED"
            if not weekend and policy_empty_count >= 3:
                action = ReconciliationAction.MANUAL_REVIEW
                reasons.append(
                    "three independent weekday empty observations lack an official holiday source"
                )
            else:
                action = ReconciliationAction.NETWORK_RECHECK
                reasons.append(
                    "empty response evidence is insufficient for non-trading classification"
                )
            if not weekend:
                warnings.append(
                    "policy v1 has no verified official PSX weekday holiday calendar"
                )
        elif current_status in _FILE_ISSUE_STATUSES:
            reconciled = current_status
            action = {
                PersistentSyncStatus.FILE_MISSING: (
                    ReconciliationAction.REPAIR_MISSING_FILE
                ),
                PersistentSyncStatus.FILE_CORRUPT: (
                    ReconciliationAction.INVESTIGATE_CORRUPT_FILE
                ),
                PersistentSyncStatus.FILE_CONFLICT: (
                    ReconciliationAction.INVESTIGATE_CONFLICT
                ),
            }[current_status]
            evidence_classification = "PERSISTED_ARTIFACT_ISSUE"
            file_state = {
                PersistentSyncStatus.FILE_MISSING: FileHealthState.MISSING,
                PersistentSyncStatus.FILE_CORRUPT: FileHealthState.CORRUPT,
                PersistentSyncStatus.FILE_CONFLICT: FileHealthState.CONFLICT,
            }[current_status]
            local_repair = True
            reasons.append("persistent artifact issue requires conservative review")
        else:
            reconciled = PersistentSyncStatus.EMPTY_UNRESOLVED
            action = ReconciliationAction.MANUAL_REVIEW
            evidence_classification = "UNSUPPORTED_PERSISTENT_STATE"
            reasons.append("available evidence does not support an automatic action")

        network_required = action in {
            ReconciliationAction.NETWORK_RECHECK,
            ReconciliationAction.REPAIR_MISSING_FILE,
        }
        eligible = False
        next_recheck_after: str | None = None
        if network_required:
            if action is ReconciliationAction.REPAIR_MISSING_FILE:
                if state is not None and state.status in _FORCEABLE_STATUSES:
                    # A normal D3 recovery may leave the historical identity
                    # intact while the current status records the latest
                    # network failure.  That attempt must govern cooldown.
                    eligible, next_recheck_after = _cooldown(
                        state,
                        evidence,
                        now=self.now,
                        cooldown_seconds=(
                            self.settings.reconciliation_cooldown_seconds
                        ),
                        force_recheck=force_recheck,
                        status=state.status,
                    )
                else:
                    stored_next = (
                        _aware_datetime(state.next_recheck_after)
                        if (
                            state is not None
                            and state.status is PersistentSyncStatus.FILE_MISSING
                            and state.recheck_policy_version == POLICY_VERSION
                        )
                        else None
                    )
                    # A prior successful fetch must not delay the first repair.
                    # Once a staged repair has actually failed, however, derive
                    # a fresh deadline even if an apply aborts before storing it.
                    failed_repair_attempt = bool(
                        state is not None
                        and state.status is PersistentSyncStatus.FILE_MISSING
                        and state.last_error_type
                        not in {
                            None,
                            "FILE_MISSING",
                            "HISTORICAL_TRADING_ARTIFACT_MISSING",
                            "PERSISTED_ARTIFACT_ISSUE",
                            "CANCELLED",
                        }
                    )
                    last_repair_attempt = (
                        None
                        if state is None
                        else _aware_datetime(state.last_attempt_at)
                    )
                    attempt_next = (
                        last_repair_attempt
                        + timedelta(
                            seconds=(
                                self.settings.reconciliation_cooldown_seconds
                            )
                        )
                        if failed_repair_attempt
                        and last_repair_attempt is not None
                        else None
                    )
                    candidates = tuple(
                        candidate
                        for candidate in (stored_next, attempt_next)
                        if candidate is not None
                    )
                    effective_next = max(candidates) if candidates else None
                    eligible = (
                        state is not None
                        and state.last_error_type == "CANCELLED"
                    ) or effective_next is None or self.now >= effective_next
                    next_recheck_after = (
                        None
                        if eligible or effective_next is None
                        else _iso(effective_next)
                    )
            else:
                eligible, next_recheck_after = _cooldown(
                    state,
                    evidence,
                    now=self.now,
                    cooldown_seconds=self.settings.reconciliation_cooldown_seconds,
                    force_recheck=force_recheck,
                    status=reconciled,
                )
            if not eligible:
                reasons.append(
                    f"network recheck is cooling down until {next_recheck_after}"
                )

        evidence_summary = ReconciliationEvidenceSummary(
            weekday=day.strftime("%A"),
            calendar_weekend=weekend,
            calendar_support=calendar_support,
            persistent_evidence=(
                None if state is None else state.evidence_state.value
            ),
            http_statuses=evidence.http_statuses,
            response_classifications=evidence.response_classifications,
            independent_empty_run_count=policy_empty_count,
            independent_valid_run_count=evidence.independent_valid_run_count,
            adjacent_previous_verified=(
                (day - timedelta(days=1)).isoformat() in adjacent_healthy
            ),
            adjacent_next_verified=(
                (day + timedelta(days=1)).isoformat() in adjacent_healthy
            ),
            expected_csv_path=str(expected_path),
            expected_checksum=expected_checksum,
            observed_checksum=inspection.checksum,
        )
        return DateReconciliationResult(
            market_date=market_date,
            previous_status=previous_status,
            reconciled_status=reconciled,
            policy_version=POLICY_VERSION,
            evidence_classification=evidence_classification,
            action_required=action,
            network_recheck_required=network_required,
            recheck_eligible_now=eligible,
            local_repair_required=local_repair,
            evidence_summary=evidence_summary,
            attempt_count=evidence.attempt_count,
            empty_observation_count=evidence.empty_observation_count,
            valid_observation_count=evidence.valid_observation_count,
            file_state=file_state,
            checksum_state=checksum_state,
            next_recheck_after=next_recheck_after,
            reasons=tuple(reasons),
            warnings=tuple(warnings),
            state_snapshot_exists=state is not None,
            state_record_updated_at=(
                None if state is None else state.record_updated_at
            ),
        )


def _range_result(
    *,
    run_id: str,
    dates: tuple[date, ...],
    mode: ReconciliationMode,
    results: tuple[DateReconciliationResult, ...],
    network_recheck_count: int = 0,
    network_rechecked_dates: tuple[str, ...] = (),
    staged_repair_dates: tuple[str, ...] = (),
    promoted_repair_dates: tuple[str, ...] = (),
    duration_ms: float = 0.0,
    warnings: tuple[str, ...] = (),
    planned_count_override: int | None = None,
    status_transition_count: int = 0,
) -> ReconciliationRangeResult:
    ordered = tuple(sorted(results, key=lambda item: item.market_date))
    statuses = Counter(item.reconciled_status for item in ordered)
    actions = Counter(item.action_required for item in ordered)
    counts_by_status = {
        status: statuses.get(status, 0) for status in PersistentSyncStatus
    }
    counts_by_action = {
        action: actions.get(action, 0) for action in ReconciliationAction
    }
    resolved_count = sum(item.resolved for item in ordered)
    network_planned = sum(item.network_recheck_required for item in ordered)
    if planned_count_override is not None:
        network_planned = planned_count_override
    manual_actions = {
        ReconciliationAction.MANUAL_REVIEW,
        ReconciliationAction.INVESTIGATE_CORRUPT_FILE,
        ReconciliationAction.INVESTIGATE_CONFLICT,
    }
    return ReconciliationRangeResult(
        run_id=run_id,
        start_date=dates[0].isoformat(),
        end_date=dates[-1].isoformat(),
        mode=mode,
        policy_version=POLICY_VERSION,
        requested_dates=tuple(day.isoformat() for day in dates),
        results=ordered,
        complete=resolved_count == len(ordered),
        resolution_percentage=(resolved_count / len(ordered)) * 100,
        counts_by_status=counts_by_status,
        counts_by_action=counts_by_action,
        verified_count=statuses.get(
            PersistentSyncStatus.VERIFIED_TRADING_DATA, 0
        ),
        confirmed_non_trading_count=statuses.get(
            PersistentSyncStatus.CONFIRMED_NON_TRADING, 0
        ),
        never_attempted_count=statuses.get(
            PersistentSyncStatus.NEVER_ATTEMPTED, 0
        ),
        unresolved_count=statuses.get(
            PersistentSyncStatus.EMPTY_UNRESOLVED, 0
        ),
        failure_count=sum(statuses.get(status, 0) for status in _FAILURE_STATUSES),
        file_health_issue_count=sum(
            statuses.get(status, 0) for status in _FILE_ISSUE_STATUSES
        ),
        network_recheck_planned_count=network_planned,
        network_recheck_count=network_recheck_count,
        local_repair_count=sum(item.local_repair_required for item in ordered),
        manual_review_count=sum(
            item.action_required in manual_actions for item in ordered
        ),
        status_transition_count=status_transition_count,
        network_rechecked_dates=tuple(sorted(network_rechecked_dates)),
        staged_repair_dates=tuple(sorted(staged_repair_dates)),
        promoted_repair_dates=tuple(sorted(promoted_repair_dates)),
        duration_ms=duration_ms,
        warnings=warnings,
    )


def _preserve_previous_statuses(
    result: ReconciliationRangeResult,
    initial: ReconciliationRangeResult,
    *,
    duration_ms: float,
    network_recheck_count: int,
    network_rechecked_dates: tuple[str, ...],
    staged_repair_dates: tuple[str, ...],
    promoted_repair_dates: tuple[str, ...],
    warnings: tuple[str, ...],
    actual_transition_count: int | None = None,
) -> ReconciliationRangeResult:
    initial_by_date = {item.market_date: item for item in initial.results}
    results = tuple(
        replace(
            item,
            previous_status=initial_by_date[item.market_date].previous_status,
        )
        for item in result.results
    )
    transitions = (
        sum(item.previous_status is not item.reconciled_status for item in results)
        if actual_transition_count is None
        else actual_transition_count
    )
    dates = tuple(date.fromisoformat(value) for value in result.requested_dates)
    return _range_result(
        run_id=result.run_id,
        dates=dates,
        mode=result.mode,
        results=results,
        network_recheck_count=network_recheck_count,
        network_rechecked_dates=network_rechecked_dates,
        staged_repair_dates=staged_repair_dates,
        promoted_repair_dates=promoted_repair_dates,
        duration_ms=duration_ms,
        warnings=warnings,
        planned_count_override=initial.network_recheck_planned_count,
        status_transition_count=transitions,
    )


class ReconciliationService:
    """Apply one auditable reconciliation run."""

    def __init__(
        self,
        settings: Settings,
        repository: StateRepository,
        *,
        client=None,
        now: datetime | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.client = client
        self.now = now or _utc_now()
        if self.now.tzinfo is None:
            self.now = self.now.replace(tzinfo=timezone.utc)
        else:
            self.now = self.now.astimezone(timezone.utc)

    async def run(
        self,
        start_date: str,
        end_date: str,
        *,
        apply: bool = False,
        force_recheck: bool = False,
        workers: int | None = None,
    ) -> ReconciliationRangeResult:
        if force_recheck and not apply:
            raise ValueError("--force-recheck requires --apply")
        requested_dates = generate_date_range(start_date, end_date)
        resolved_workers = validate_workers(
            self.settings.range_workers if workers is None else workers,
            self.settings,
        )
        self.repository.initialize()
        mode = ReconciliationMode.APPLY if apply else ReconciliationMode.DRY_RUN
        run_id = self.repository.begin_reconciliation_run(
            policy_version=POLICY_VERSION,
            start_date=start_date,
            end_date=end_date,
            mode=mode,
            requested_date_count=len(requested_dates),
            worker_count=resolved_workers,
            force_recheck=force_recheck,
            max_rechecks_per_date=self.settings.max_rechecks_per_date_per_run,
            cooldown_seconds=self.settings.reconciliation_cooldown_seconds,
        )
        started = time.perf_counter()
        planner = ReconciliationPlanner(
            self.settings, self.repository, now=self.now
        )
        child_sync_run_id: str | None = None
        child_finished = False
        claimed_dates: tuple[str, ...] = ()
        applied_actions: dict[str, ReconciliationAction] = {}
        network_results: dict[str, DownloadResult] = {}
        attempted_dates: set[str] = set()
        staged_dates: set[str] = set()
        promoted_dates: set[str] = set()
        run_warnings: list[str] = []
        initial: ReconciliationRangeResult | None = None

        try:
            if apply:
                recovered = self.repository.recover_pending_repair_candidates(
                    start_date,
                    end_date,
                    self.settings.raw_output_dir,
                    reconciliation_run_id=run_id,
                )
                if recovered:
                    promoted_dates.update(
                        market_date
                        for market_date, disposition in recovered
                        if disposition == "RECOVERY_PROMOTED"
                    )
                    run_warnings.append(
                        "recovered durable repair audit outcomes: "
                        + ", ".join(
                            f"{market_date}={disposition}"
                            for market_date, disposition in recovered
                        )
                    )
            initial = planner.plan(
                requested_dates,
                run_id=run_id,
                mode=mode,
                force_recheck=force_recheck,
            )
            if not apply:
                completed = replace(
                    initial,
                    duration_ms=(time.perf_counter() - started) * 1000,
                )
                self.repository.finish_reconciliation_run(completed)
                return completed

            # Re-plan each local decision immediately before its guarded,
            # event-atomic mutation.
            for item in initial.results:
                if not (
                    item.action_required
                    in {
                        ReconciliationAction.LOCAL_REINDEX,
                        ReconciliationAction.CONFIRM_NON_TRADING,
                        ReconciliationAction.MANUAL_REVIEW,
                    }
                    or item.reconciled_status in _FILE_ISSUE_STATUSES
                ):
                    continue
                market_day = date.fromisoformat(item.market_date)
                fresh = planner.plan(
                    (market_day,),
                    run_id=run_id,
                    mode=mode,
                    force_recheck=force_recheck,
                ).results[0]
                if self._apply_local_decision(run_id, fresh):
                    applied_actions[item.market_date] = fresh.action_required
                elif fresh.action_required is ReconciliationAction.MANUAL_REVIEW:
                    self.repository.record_reconciliation_event(run_id, fresh)
                    applied_actions[item.market_date] = fresh.action_required

            pre_network = planner.plan(
                requested_dates,
                run_id=run_id,
                mode=mode,
                force_recheck=force_recheck,
            )
            pre_network_by_date = {
                item.market_date: item for item in pre_network.results
            }
            initial_states = self.repository.get_date_states_for_range(
                start_date, end_date
            )
            eligible_dates = tuple(
                item.market_date
                for item in pre_network.results
                if item.network_recheck_required and item.recheck_eligible_now
            )
            claim_time = self.now
            batches = (
                (len(eligible_dates) + resolved_workers - 1) // resolved_workers
                if eligible_dates
                else 0
            )
            estimated_lease = timedelta(
                seconds=(
                    batches
                    * (
                        self.settings.request_timeout_seconds
                        * self.settings.max_rechecks_per_date_per_run
                        + self.settings.retry_backoff_max_seconds
                        * max(self.settings.max_rechecks_per_date_per_run - 1, 0)
                        + 60
                    )
                )
            )
            claim_lease = max(RECHECK_CLAIM_LEASE, estimated_lease)
            claimed_dates = self.repository.claim_network_rechecks(
                run_id,
                eligible_dates,
                claimed_at=_iso(claim_time),
                expires_at=_iso(claim_time + claim_lease),
            )
            if len(claimed_dates) != len(eligible_dates):
                skipped = sorted(set(eligible_dates) - set(claimed_dates))
                run_warnings.append(
                    "concurrent reconciliation already claimed: "
                    + ", ".join(skipped)
                )

            if claimed_dates:
                target_days = tuple(date.fromisoformat(value) for value in claimed_dates)
                child_sync_run_id = self.repository.begin_sync_run(
                    "reconcile-recheck",
                    min(claimed_dates),
                    max(claimed_dates),
                    len(claimed_dates),
                    resolved_workers,
                )
                self.repository.link_reconciliation_sync_run(
                    run_id, child_sync_run_id
                )
                staging_dir = self.settings.repair_staging_dir / run_id
                staged_settings = replace(
                    self.settings,
                    raw_output_dir=staging_dir,
                    retry_attempts=self.settings.max_rechecks_per_date_per_run,
                )
                async_repository = AsyncStateRepository(self.repository)

                async def observe_attempt(event) -> None:
                    # HTTP already occurred before the observer runs. Count it
                    # even if cancellation arrives while its durable audit row
                    # is being drained.
                    attempted_dates.add(event.requested_date)
                    await async_repository.record_staged_attempt(
                        child_sync_run_id, event
                    )

                async def observe_result(result: DownloadResult) -> None:
                    if result.locally_skipped:
                        await async_repository.record_staged_download_result(
                            child_sync_run_id, result
                        )
                        network_results[result.requested_date] = result
                        applied_actions[result.requested_date] = (
                            pre_network_by_date[
                                result.requested_date
                            ].action_required
                        )
                        return
                    await async_repository.run_serialized(
                        self._record_and_adjudicate_network_result,
                        child_sync_run_id,
                        run_id,
                        result,
                        pre_network_by_date[result.requested_date],
                        initial_states.get(result.requested_date),
                        staging_dir,
                        staged_dates,
                        promoted_dates,
                    )
                    network_results[result.requested_date] = result
                    applied_actions[result.requested_date] = pre_network_by_date[
                        result.requested_date
                    ].action_required

                async def execute(client) -> None:
                    downloader = ConcurrentRangeDownloader(
                        staged_settings,
                        client,
                        workers=resolved_workers,
                        preflight=lambda day: async_repository.run_serialized(
                            self.repository.prepare_fetch,
                            day,
                            self.settings.raw_output_dir,
                            self.settings.canonical_columns,
                            allow_staged_repair=True,
                            mutate_state=False,
                        ),
                        attempt_observer=observe_attempt,
                        result_observer=observe_result,
                    )
                    await downloader.download_dates(target_days)

                if self.client is None:
                    async with AsyncPSXClient(
                        staged_settings, workers=resolved_workers
                    ) as client:
                        await execute(client)
                else:
                    await execute(self.client)
                self.repository.finish_sync_run(child_sync_run_id)
                child_finished = True

                for market_date in network_results:
                    current = self.repository.get_date_state(market_date)
                    if current is None:
                        continue
                    if current.status in {
                        PersistentSyncStatus.EMPTY_UNRESOLVED,
                        *_FAILURE_STATUSES,
                    } or current.status is PersistentSyncStatus.FILE_MISSING:
                        last_attempt = _aware_datetime(current.last_attempt_at)
                        next_recheck = (
                            None
                            if last_attempt is None
                            else _iso(
                                last_attempt
                                + timedelta(
                                    seconds=self.settings.reconciliation_cooldown_seconds
                                )
                            )
                        )
                        self.repository.set_recheck_after(
                            market_date,
                            next_recheck,
                            POLICY_VERSION,
                            expected_record_updated_at=current.record_updated_at,
                            expected_status=current.status,
                        )
                    else:
                        self.repository.set_recheck_after(
                            market_date,
                            None,
                            POLICY_VERSION,
                            expected_record_updated_at=current.record_updated_at,
                            expected_status=current.status,
                        )

            # Network work can introduce a second independent weekend empty, a
            # new local artifact, or a candidate conflict. Converge those
            # non-network conclusions in the same apply run.
            interim = planner.plan(
                requested_dates,
                run_id=run_id,
                mode=mode,
                force_recheck=force_recheck,
            )
            for item in interim.results:
                if not (
                    item.action_required
                    in {
                        ReconciliationAction.CONFIRM_NON_TRADING,
                        ReconciliationAction.LOCAL_REINDEX,
                    }
                    or item.reconciled_status in _FILE_ISSUE_STATUSES
                ):
                    continue
                market_day = date.fromisoformat(item.market_date)
                fresh = planner.plan(
                    (market_day,),
                    run_id=run_id,
                    mode=mode,
                    force_recheck=force_recheck,
                ).results[0]
                if self._apply_local_decision(run_id, fresh):
                    applied_actions[item.market_date] = fresh.action_required

            final_plan = planner.plan(
                requested_dates,
                run_id=run_id,
                mode=mode,
                force_recheck=force_recheck,
            )
            completed = _preserve_previous_statuses(
                final_plan,
                initial,
                duration_ms=(time.perf_counter() - started) * 1000,
                network_recheck_count=len(attempted_dates),
                network_rechecked_dates=tuple(attempted_dates),
                staged_repair_dates=tuple(staged_dates),
                promoted_repair_dates=tuple(promoted_dates),
                warnings=tuple(run_warnings),
                actual_transition_count=self._actual_transition_count(initial),
            )
            self._record_applied_events(initial, completed, applied_actions)
            self.repository.finish_reconciliation_run(completed)
            return completed
        except BaseException as exc:
            interrupted = isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError))
            if interrupted:
                # A cancellation during retry backoff has no separate network
                # attempt to record.  Mark every attempted-but-unfinished date
                # as resumable before computing the terminal snapshot.
                for market_date in sorted(
                    attempted_dates.difference(network_results)
                ):
                    try:
                        self.repository.mark_reconciliation_date_cancelled(
                            market_date
                        )
                    except Exception as marker_exc:
                        run_warnings.append(
                            f"could not persist cancellation marker for "
                            f"{market_date}: {marker_exc}"
                        )
            if child_sync_run_id is not None and not child_finished:
                try:
                    self.repository.finish_sync_run(
                        child_sync_run_id, interrupted=True
                    )
                except Exception:
                    pass
            try:
                if initial is None:
                    raise RuntimeError("initial reconciliation plan was not built")
                partial_plan = planner.plan(
                    requested_dates,
                    run_id=run_id,
                    mode=mode,
                    force_recheck=force_recheck,
                )
                partial = _preserve_previous_statuses(
                    partial_plan,
                    initial,
                    duration_ms=(time.perf_counter() - started) * 1000,
                    network_recheck_count=len(attempted_dates),
                    network_rechecked_dates=tuple(attempted_dates),
                    staged_repair_dates=tuple(staged_dates),
                    promoted_repair_dates=tuple(promoted_dates),
                    warnings=tuple(run_warnings),
                    actual_transition_count=self._actual_transition_count(initial),
                )
                terminal_error = str(exc) or (
                    "interrupted" if interrupted else type(exc).__name__
                )
                try:
                    self._record_applied_events(initial, partial, applied_actions)
                except Exception as audit_exc:
                    terminal_error = (
                        f"{terminal_error}; reconciliation event finalization "
                        f"also failed: {audit_exc}"
                    )
                if interrupted:
                    self.repository.finish_reconciliation_run(
                        partial,
                        interrupted=True,
                        error_message=terminal_error,
                    )
                else:
                    self.repository.finish_reconciliation_run(
                        partial,
                        failed=True,
                        error_message=terminal_error,
                    )
            except Exception:
                try:
                    self.repository.mark_reconciliation_run_failed(
                        run_id,
                        interrupted=interrupted,
                        duration_ms=(time.perf_counter() - started) * 1000,
                        error_message=str(exc),
                    )
                except Exception:
                    pass
            raise
        finally:
            if claimed_dates:
                self.repository.release_network_rechecks(run_id)

    def _apply_local_decision(
        self,
        reconciliation_run_id: str,
        item: DateReconciliationResult,
    ) -> bool:
        """Apply one freshly planned local decision with an optimistic guard."""

        market_day = date.fromisoformat(item.market_date)
        if item.action_required is ReconciliationAction.LOCAL_REINDEX:
            outcome = self.repository.prepare_fetch(
                market_day,
                self.settings.raw_output_dir,
                self.settings.canonical_columns,
                expected_record_updated_at=item.state_record_updated_at,
                expected_state_exists=item.state_snapshot_exists,
                reconciliation_run_id=reconciliation_run_id,
                reconciliation_decision=item,
                expected_artifact_path=Path(
                    item.evidence_summary.expected_csv_path
                ),
                expected_artifact_exists=True,
                expected_artifact_valid=True,
                expected_observed_checksum=(
                    item.evidence_summary.observed_checksum
                ),
            )
            return outcome is not None and outcome.successful
        if item.action_required is ReconciliationAction.CONFIRM_NON_TRADING:
            if not item.state_snapshot_exists:
                raise RuntimeError(
                    "non-trading evidence unexpectedly lacks persistent state"
                )
            return self.repository.confirm_non_trading(
                item.market_date,
                policy_version=POLICY_VERSION,
                classification_basis=item.evidence_classification,
                expected_record_updated_at=item.state_record_updated_at,
                expected_state_exists=item.state_snapshot_exists,
                canonical_path=(
                    self.settings.raw_output_dir
                    / f"market_{item.market_date}.csv"
                ),
                reconciliation_run_id=reconciliation_run_id,
                reconciliation_decision=item,
            )
        if item.reconciled_status in _FILE_ISSUE_STATUSES:
            # Persisted artifact conclusions are already the conservative end
            # state.  Re-applying the generic planner diagnosis would replace
            # a more precise repair-adjudication error (for example,
            # HISTORICAL_MISMATCH) and create a misleading no-op event.
            if (
                item.state_snapshot_exists
                and item.previous_status is item.reconciled_status
            ):
                return False
            artifact_path = (
                self.settings.raw_output_dir
                / f"market_{item.market_date}.csv"
            )
            expected_exists = item.reconciled_status is not (
                PersistentSyncStatus.FILE_MISSING
            )
            if (
                item.reconciled_status is PersistentSyncStatus.FILE_CONFLICT
                and item.evidence_summary.observed_checksum is None
            ):
                expected_exists = False
            return self.repository.mark_artifact_issue(
                item.market_date,
                item.reconciled_status,
                error_type=item.evidence_classification,
                error_message="; ".join(item.reasons),
                expected_record_updated_at=item.state_record_updated_at,
                expected_state_exists=item.state_snapshot_exists,
                reconciliation_run_id=reconciliation_run_id,
                reconciliation_decision=item,
                expected_artifact_path=artifact_path,
                expected_artifact_exists=expected_exists,
                expected_artifact_valid=(
                    expected_exists
                    and item.reconciled_status
                    is PersistentSyncStatus.FILE_CONFLICT
                ),
                expected_observed_checksum=(
                    item.evidence_summary.observed_checksum
                    if expected_exists
                    else None
                ),
            )
        return False

    def _actual_transition_count(
        self, initial: ReconciliationRangeResult
    ) -> int:
        events = self.repository.list_reconciliation_events(initial.run_id)
        event_dates = {event["market_date"] for event in events}
        committed_event_transitions = sum(
            event["previous_status"] != event["new_status"] for event in events
        )
        states = self.repository.get_date_states_for_range(
            initial.start_date, initial.end_date
        )
        unaudited_terminal_transitions = sum(
            item.previous_status
            is not (
                PersistentSyncStatus.NEVER_ATTEMPTED
                if item.market_date not in states
                else _normalized_status(states[item.market_date].status)
            )
            for item in initial.results
            if item.market_date not in event_dates
        )
        return committed_event_transitions + unaudited_terminal_transitions

    def _adjudicate_network_result(
        self,
        reconciliation_run_id: str,
        result: DownloadResult,
        initial: DateReconciliationResult,
        initial_state: DateSyncState | None,
        staging_dir: Path,
        staged_dates: set[str],
        promoted_dates: set[str],
    ) -> None:
        staged_path = (
            result.saved_path
            if result.saved_path is not None
            else staging_dir / f"market_{result.requested_date}.csv"
        )
        prior_checksum = (
            initial.evidence_summary.expected_checksum
            or (
                None
                if initial_state is None
                else initial_state.csv_checksum_sha256
            )
        )
        prior_rows = (
            None
            if initial_state is None or initial_state.valid_row_count <= 0
            else initial_state.valid_row_count
        )
        historical_repair = (
            initial.action_required is ReconciliationAction.REPAIR_MISSING_FILE
            or initial.reconciled_status is PersistentSyncStatus.FILE_MISSING
        )

        if not result.successful or result.saved_path is None:
            self.repository.record_repair_candidate(
                reconciliation_run_id,
                result.requested_date,
                staged_path,
                prior_checksum=prior_checksum,
                candidate_checksum=result.checksum,
                prior_row_count=prior_rows,
                candidate_row_count=(result.valid_row_count or None),
                validation_state=result.status.value,
                disposition="NO_VALID_STAGED_ARTIFACT",
                message=result.error,
            )
            return

        staged_dates.add(result.requested_date)
        destination = (
            self.settings.raw_output_dir
            / f"market_{result.requested_date}.csv"
        )
        staged_inspection = inspect_canonical_csv_file(
            staged_path, self.settings.canonical_columns
        )
        if (
            not staged_inspection.valid
            or staged_inspection.checksum is None
            or staged_inspection.row_count <= 0
        ):
            self.repository.record_repair_candidate(
                reconciliation_run_id,
                result.requested_date,
                staged_path,
                prior_checksum=prior_checksum,
                candidate_checksum=staged_inspection.checksum,
                prior_row_count=prior_rows,
                candidate_row_count=(staged_inspection.row_count or None),
                validation_state="INVALID",
                disposition=StagedPromotionStatus.STAGED_FILE_INVALID.value,
                message=(
                    staged_inspection.error
                    or "successful download no longer has a valid staged artifact"
                ),
            )
            return

        # A row count from a failed first download is observation evidence, not
        # a trusted historical artifact identity.  Only historical repairs need
        # the checksum/row-count pair to be complete.
        complete_historical_identity = not historical_repair or (
            (prior_checksum is None) == (prior_rows is None)
        )
        if not complete_historical_identity:
            promotion = promote_staged_csv_if_safe(
                staged_path,
                destination,
                expected_checksum=prior_checksum if historical_repair else None,
                expected_row_count=prior_rows if historical_repair else None,
                allow_new=not historical_repair,
                columns=self.settings.canonical_columns,
            )
            self.repository.record_repair_candidate(
                reconciliation_run_id,
                result.requested_date,
                staged_path,
                prior_checksum=prior_checksum,
                candidate_checksum=staged_inspection.checksum,
                prior_row_count=prior_rows,
                candidate_row_count=staged_inspection.row_count,
                validation_state="VALID",
                disposition=promotion.status.value,
                message=promotion.message,
            )
            if historical_repair and promotion.status is (
                StagedPromotionStatus.POLICY_REJECTED
            ):
                current = self.repository.get_date_state(result.requested_date)
                decision = replace(
                    initial,
                    reconciled_status=PersistentSyncStatus.FILE_CONFLICT,
                    evidence_classification=promotion.status.value,
                    file_state=FileHealthState.CONFLICT,
                    checksum_state=ChecksumState.MISMATCH,
                    network_recheck_required=False,
                    recheck_eligible_now=False,
                    local_repair_required=True,
                    reasons=(promotion.message,),
                )
                self.repository.mark_artifact_issue(
                    result.requested_date,
                    PersistentSyncStatus.FILE_CONFLICT,
                    error_type=promotion.status.value,
                    error_message=promotion.message,
                    expected_record_updated_at=(
                        None if current is None else current.record_updated_at
                    ),
                    expected_state_exists=current is not None,
                    reconciliation_run_id=reconciliation_run_id,
                    reconciliation_decision=decision,
                    expected_artifact_path=destination,
                    expected_artifact_exists=False,
                    expected_artifact_valid=False,
                )
            return

        self.repository.begin_repair_candidate(
            reconciliation_run_id,
            result.requested_date,
            staged_path,
            prior_checksum=prior_checksum if historical_repair else None,
            candidate_checksum=staged_inspection.checksum,
            prior_row_count=prior_rows if historical_repair else None,
            candidate_row_count=staged_inspection.row_count,
        )
        self.repository.authorize_pending_repair_promotion(
            reconciliation_run_id,
            result.requested_date,
            staged_path,
            destination,
        )
        promotion = promote_staged_csv_if_safe(
            staged_path,
            destination,
            expected_checksum=prior_checksum if historical_repair else None,
            expected_row_count=prior_rows if historical_repair else None,
            allow_new=not historical_repair,
            columns=self.settings.canonical_columns,
        )
        if promotion.promoted:
            promoted_dates.add(result.requested_date)
            current_state = self.repository.get_date_state(
                result.requested_date
            )
            if current_state is None:
                raise StateDatabaseError(
                    "promoted repair lost its persistent state for "
                    f"{result.requested_date}"
                )
            decision = replace(
                initial,
                previous_status=_normalized_status(current_state.status),
                reconciled_status=PersistentSyncStatus.VERIFIED_TRADING_DATA,
                evidence_classification="STAGED_CANONICAL_PROMOTED",
                file_state=FileHealthState.HEALTHY,
                checksum_state=ChecksumState.MATCH,
                network_recheck_required=False,
                recheck_eligible_now=False,
                local_repair_required=False,
                reasons=(promotion.message,),
            )
            self.repository.finalize_promoted_repair(
                reconciliation_run_id,
                result.requested_date,
                destination,
                decision,
                message=promotion.message,
            )
        elif historical_repair and promotion.status in {
            StagedPromotionStatus.HISTORICAL_MISMATCH,
            StagedPromotionStatus.POLICY_REJECTED,
        }:
            current_state = self.repository.get_date_state(
                result.requested_date
            )
            if current_state is None:
                raise StateDatabaseError(
                    "rejected repair lost its persistent state for "
                    f"{result.requested_date}"
                )
            decision = replace(
                initial,
                previous_status=_normalized_status(current_state.status),
                reconciled_status=PersistentSyncStatus.FILE_CONFLICT,
                evidence_classification=promotion.status.value,
                file_state=FileHealthState.CONFLICT,
                checksum_state=ChecksumState.MISMATCH,
                network_recheck_required=False,
                recheck_eligible_now=False,
                local_repair_required=True,
                reasons=(promotion.message,),
            )
            self.repository.finalize_rejected_repair(
                reconciliation_run_id,
                result.requested_date,
                destination,
                decision,
                validation_state="VALID",
                disposition=promotion.status.value,
                message=promotion.message,
            )
        elif promotion.status is StagedPromotionStatus.DESTINATION_ALREADY_EXISTS:
            raise StateDatabaseError(
                "canonical destination appeared during promotion for "
                f"{result.requested_date}; durable intent remains pending"
            )
        else:
            self.repository.finalize_repair_candidate(
                reconciliation_run_id,
                result.requested_date,
                validation_state=(
                    "INVALID"
                    if promotion.status is StagedPromotionStatus.STAGED_FILE_INVALID
                    else "VALID"
                ),
                disposition=promotion.status.value,
                message=promotion.message,
            )

    def _record_and_adjudicate_network_result(
        self,
        child_sync_run_id: str,
        reconciliation_run_id: str,
        result: DownloadResult,
        initial: DateReconciliationResult,
        initial_state: DateSyncState | None,
        staging_dir: Path,
        staged_dates: set[str],
        promoted_dates: set[str],
    ) -> None:
        """Persist and adjudicate one result under the async writer lock."""

        self.repository.record_staged_download_result(
            child_sync_run_id, result
        )
        self._adjudicate_network_result(
            reconciliation_run_id,
            result,
            initial,
            initial_state,
            staging_dir,
            staged_dates,
            promoted_dates,
        )

    def _record_applied_events(
        self,
        initial: ReconciliationRangeResult,
        final: ReconciliationRangeResult,
        actions: dict[str, ReconciliationAction],
    ) -> None:
        initial_by_date = {item.market_date: item for item in initial.results}
        final_by_date = {item.market_date: item for item in final.results}
        for market_date in sorted(actions):
            before = initial_by_date[market_date]
            after = final_by_date[market_date]
            event = replace(
                after,
                previous_status=before.previous_status,
                action_required=actions[market_date],
            )
            self.repository.record_reconciliation_event(initial.run_id, event)


async def reconcile_range(
    settings: Settings,
    repository: StateRepository,
    start_date: str,
    end_date: str,
    *,
    apply: bool = False,
    force_recheck: bool = False,
    workers: int | None = None,
    client=None,
    now: datetime | None = None,
) -> ReconciliationRangeResult:
    """Async convenience entry point for application and tests."""

    return await ReconciliationService(
        settings,
        repository,
        client=client,
        now=now,
    ).run(
        start_date,
        end_date,
        apply=apply,
        force_recheck=force_recheck,
        workers=workers,
    )


def run_reconciliation(
    settings: Settings,
    repository: StateRepository,
    start_date: str,
    end_date: str,
    *,
    apply: bool = False,
    force_recheck: bool = False,
    workers: int | None = None,
    client=None,
    now: datetime | None = None,
) -> ReconciliationRangeResult:
    """Synchronous CLI entry point."""

    return asyncio.run(
        reconcile_range(
            settings,
            repository,
            start_date,
            end_date,
            apply=apply,
            force_recheck=force_recheck,
            workers=workers,
            client=client,
            now=now,
        )
    )
