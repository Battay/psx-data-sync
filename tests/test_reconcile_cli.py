from __future__ import annotations

import json

from typer.testing import CliRunner

import psx_data_sync.cli as cli
from psx_data_sync.state import (
    ChecksumState,
    DateReconciliationResult,
    FileHealthState,
    PersistentSyncStatus,
    ReconciliationAction,
    ReconciliationEvidenceSummary,
    ReconciliationMode,
    ReconciliationRangeResult,
)


runner = CliRunner()
POLICY = "psx_reconciliation_policy_v1"


def date_result(
    market_date: str,
    status: PersistentSyncStatus,
    action: ReconciliationAction,
) -> DateReconciliationResult:
    healthy = status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    return DateReconciliationResult(
        market_date=market_date,
        previous_status=status,
        reconciled_status=status,
        policy_version=POLICY,
        evidence_classification=(
            "VALIDATED_TRADING_DATA" if healthy else "INSUFFICIENT_EVIDENCE"
        ),
        action_required=action,
        network_recheck_required=action is ReconciliationAction.NETWORK_RECHECK,
        recheck_eligible_now=action is ReconciliationAction.NETWORK_RECHECK,
        local_repair_required=False,
        evidence_summary=ReconciliationEvidenceSummary(
            weekday="Tuesday" if healthy else "Wednesday",
            calendar_weekend=False,
            calendar_support=None,
            persistent_evidence=(
                "NETWORK_VALIDATED_CSV" if healthy else "NETWORK_OBSERVATION"
            ),
            http_statuses=(200,),
            response_classifications=(
                "EQUITY_ROWS" if healthy else "EMPTY_MARKET_RESPONSE",
            ),
            independent_empty_run_count=0 if healthy else 1,
            independent_valid_run_count=1 if healthy else 0,
            adjacent_previous_verified=False,
            adjacent_next_verified=False,
            expected_csv_path=f"data/raw/market_{market_date}.csv",
            expected_checksum="abc123" if healthy else None,
            observed_checksum="abc123" if healthy else None,
        ),
        attempt_count=1,
        empty_observation_count=0 if healthy else 1,
        valid_observation_count=1 if healthy else 0,
        file_state=(
            FileHealthState.HEALTHY
            if healthy
            else FileHealthState.NOT_APPLICABLE
        ),
        checksum_state=(
            ChecksumState.MATCH
            if healthy
            else ChecksumState.NOT_APPLICABLE
        ),
        reasons=("validated local artifact",) if healthy else ("one empty response",),
    )


def range_result(*, complete: bool) -> ReconciliationRangeResult:
    results = (
        date_result(
            "2026-08-04",
            PersistentSyncStatus.VERIFIED_TRADING_DATA,
            ReconciliationAction.NO_ACTION,
        ),
        date_result(
            "2026-08-05",
            PersistentSyncStatus.EMPTY_UNRESOLVED,
            ReconciliationAction.NETWORK_RECHECK,
        ),
    )
    return ReconciliationRangeResult(
        run_id="reconcile-run-1",
        start_date="2026-08-04",
        end_date="2026-08-05",
        mode=ReconciliationMode.DRY_RUN,
        policy_version=POLICY,
        requested_dates=("2026-08-04", "2026-08-05"),
        results=results,
        complete=complete,
        resolution_percentage=100.0 if complete else 50.0,
        counts_by_status={
            PersistentSyncStatus.VERIFIED_TRADING_DATA: 1,
            PersistentSyncStatus.EMPTY_UNRESOLVED: 1,
        },
        counts_by_action={
            ReconciliationAction.NO_ACTION: 1,
            ReconciliationAction.NETWORK_RECHECK: 1,
        },
        verified_count=1,
        confirmed_non_trading_count=0,
        never_attempted_count=0,
        unresolved_count=0 if complete else 1,
        failure_count=0,
        file_health_issue_count=0,
        network_recheck_planned_count=1,
        network_recheck_count=0,
        local_repair_count=0,
        manual_review_count=0,
        status_transition_count=0,
    )


def test_reconcile_help_lists_safe_apply_and_report_options() -> None:
    result = runner.invoke(cli.app, ["reconcile", "--help"])

    assert result.exit_code == 0
    for option in (
        "--start",
        "--end",
        "--apply",
        "--force-recheck",
        "--only-problems",
        "--status",
        "--json",
        "--workers",
    ):
        assert option in result.output


def test_reconcile_is_dry_run_by_default(monkeypatch) -> None:
    captured = {}

    def mocked(start_date, end_date, workers, **kwargs):
        captured.update(
            start_date=start_date,
            end_date=end_date,
            workers=workers,
            **kwargs,
        )
        return range_result(complete=True)

    monkeypatch.setattr(cli, "run_reconciliation", mocked)
    result = runner.invoke(
        cli.app,
        ["reconcile", "--start", "2026-08-04", "--end", "2026-08-05"],
    )

    assert result.exit_code == 0
    assert captured["apply"] is False
    assert captured["force_recheck"] is False
    assert captured["workers"] == 4
    assert "Mode: DRY RUN" in result.output
    assert "INCOMPLETE" not in result.output


def test_reconcile_json_is_clean_and_filters_dates_not_summary(monkeypatch) -> None:
    monkeypatch.setattr(
        cli,
        "run_reconciliation",
        lambda *args, **kwargs: range_result(complete=False),
    )

    result = runner.invoke(
        cli.app,
        [
            "reconcile",
            "--start",
            "2026-08-04",
            "--end",
            "2026-08-05",
            "--only-problems",
            "--status",
            "EMPTY_UNRESOLVED",
            "--json",
        ],
    )

    assert result.exit_code == 3
    payload = json.loads(result.output)
    assert all(
        "state_snapshot_exists" not in item
        and "state_record_updated_at" not in item
        for item in payload["results"]
    )
    assert payload["range"] == {
        "start_date": "2026-08-04",
        "end_date": "2026-08-05",
        "requested_date_count": 2,
        "displayed_date_count": 1,
    }
    assert payload["complete"] is False
    assert payload["summary"]["verified_count"] == 1
    assert payload["summary"]["unresolved_count"] == 1
    assert payload["filters"]["status"] == "EMPTY_UNRESOLVED"
    assert [item["market_date"] for item in payload["results"]] == [
        "2026-08-05"
    ]
    assert payload["results"][0]["action_required"] == "NETWORK_RECHECK"
    assert payload["results"][0]["has_problem"] is True


def test_force_recheck_requires_apply_and_does_not_run_service(monkeypatch) -> None:
    def should_not_run(*args, **kwargs):
        raise AssertionError("service should not run")

    monkeypatch.setattr(cli, "run_reconciliation", should_not_run)
    result = runner.invoke(
        cli.app,
        [
            "reconcile",
            "--start",
            "2026-08-04",
            "--end",
            "2026-08-05",
            "--force-recheck",
            "--json",
        ],
    )

    assert result.exit_code == 2
    assert json.loads(result.output) == {
        "category": "Input error",
        "error": "--force-recheck requires --apply",
    }


def test_apply_and_force_are_forwarded(monkeypatch) -> None:
    captured = {}

    def mocked(*args, **kwargs):
        captured.update(kwargs)
        return range_result(complete=True)

    monkeypatch.setattr(cli, "run_reconciliation", mocked)
    result = runner.invoke(
        cli.app,
        [
            "reconcile",
            "--start",
            "2026-08-04",
            "--end",
            "2026-08-05",
            "--apply",
            "--force-recheck",
            "--workers",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert captured["apply"] is True
    assert captured["force_recheck"] is True
    assert "Date-level reconciliation plan" in result.output


def test_reconcile_reversed_range_is_input_error(monkeypatch) -> None:
    monkeypatch.setattr(
        cli,
        "run_reconciliation",
        lambda *args, **kwargs: range_result(complete=True),
    )
    result = runner.invoke(
        cli.app,
        ["reconcile", "--start", "2026-08-05", "--end", "2026-08-04"],
    )

    assert result.exit_code == 2
    assert "start date" in result.output


def test_reconcile_catastrophe_and_interrupt_have_distinct_exit_codes(
    monkeypatch,
) -> None:
    def catastrophe(*args, **kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(cli, "run_reconciliation", catastrophe)
    failed = runner.invoke(
        cli.app,
        [
            "reconcile",
            "--start",
            "2026-08-04",
            "--end",
            "2026-08-05",
            "--json",
        ],
    )
    assert failed.exit_code == 1
    assert json.loads(failed.output)["category"] == "Reconciliation error"

    def interrupted(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "run_reconciliation", interrupted)
    stopped = runner.invoke(
        cli.app,
        [
            "reconcile",
            "--start",
            "2026-08-04",
            "--end",
            "2026-08-05",
            "--json",
        ],
    )
    assert stopped.exit_code == 130
    assert json.loads(stopped.output)["category"] == "Interrupted"
