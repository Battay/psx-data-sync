from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from PySide6.QtWidgets import QApplication, QMessageBox

from psx_data_sync.config import DEFAULT_RANGE_WORKERS
from psx_data_sync.gui.app import create_app
from psx_data_sync.gui.reconciliation_panel import ReconciliationWidget
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
from psx_data_sync.state_db import StateRepository

os.environ["QT_QPA_PLATFORM"] = "offscreen"


@pytest.fixture(scope="session")
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = create_app(["--offscreen"])
    return app


def _dummy_reconciliation_result(
    start_date: str = "2026-08-01",
    end_date: str = "2026-08-02",
    mode: ReconciliationMode = ReconciliationMode.DRY_RUN,
) -> ReconciliationRangeResult:
    res1 = DateReconciliationResult(
        market_date="2026-08-01",
        previous_status=PersistentSyncStatus.NEVER_ATTEMPTED,
        reconciled_status=PersistentSyncStatus.VERIFIED_TRADING_DATA,
        policy_version="v1",
        evidence_classification="EQUITY_ROWS",
        action_required=ReconciliationAction.NO_ACTION,
        network_recheck_required=False,
        recheck_eligible_now=False,
        local_repair_required=False,
        evidence_summary=ReconciliationEvidenceSummary(
            weekday="Saturday",
            calendar_weekend=False,
            calendar_support=None,
            persistent_evidence="LOCAL_CSV_SHA256_VERIFIED",
            http_statuses=(200,),
            response_classifications=("EQUITY_ROWS",),
            independent_empty_run_count=0,
            independent_valid_run_count=1,
            adjacent_previous_verified=True,
            adjacent_next_verified=True,
            expected_csv_path="data/raw/market_2026-08-01.csv",
            expected_checksum="abc1",
            observed_checksum="abc1",
        ),
        attempt_count=1,
        empty_observation_count=0,
        valid_observation_count=1,
        file_state=FileHealthState.HEALTHY,
        checksum_state=ChecksumState.MATCH,
    )
    res2 = DateReconciliationResult(
        market_date="2026-08-02",
        previous_status=PersistentSyncStatus.NEVER_ATTEMPTED,
        reconciled_status=PersistentSyncStatus.CONFIRMED_NON_TRADING,
        policy_version="v1",
        evidence_classification="WEEKEND_CALENDAR",
        action_required=ReconciliationAction.CONFIRM_NON_TRADING,
        network_recheck_required=False,
        recheck_eligible_now=False,
        local_repair_required=False,
        evidence_summary=ReconciliationEvidenceSummary(
            weekday="Sunday",
            calendar_weekend=True,
            calendar_support="WEEKEND",
            persistent_evidence=None,
            http_statuses=(),
            response_classifications=(),
            independent_empty_run_count=0,
            independent_valid_run_count=0,
            adjacent_previous_verified=True,
            adjacent_next_verified=True,
            expected_csv_path="",
            expected_checksum=None,
            observed_checksum=None,
        ),
        attempt_count=0,
        empty_observation_count=0,
        valid_observation_count=0,
        file_state=FileHealthState.NOT_APPLICABLE,
        checksum_state=ChecksumState.NOT_APPLICABLE,
    )
    return ReconciliationRangeResult(
        run_id="rec_123",
        start_date=start_date,
        end_date=end_date,
        mode=mode,
        policy_version="v1",
        requested_dates=(start_date, end_date),
        results=(res1, res2),
        complete=True,
        resolution_percentage=100.0,
        counts_by_status={
            PersistentSyncStatus.VERIFIED_TRADING_DATA: 1,
            PersistentSyncStatus.CONFIRMED_NON_TRADING: 1,
        },
        counts_by_action={
            ReconciliationAction.NO_ACTION: 1,
            ReconciliationAction.CONFIRM_NON_TRADING: 1,
        },
        verified_count=1,
        confirmed_non_trading_count=1,
        never_attempted_count=0,
        unresolved_count=0,
        failure_count=0,
        file_health_issue_count=0,
        network_recheck_planned_count=0,
        network_recheck_count=0,
        local_repair_count=0,
        manual_review_count=0,
        status_transition_count=1,
        duration_ms=850.0,
    )


def test_reconciliation_widget_construction_no_network(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    with patch("psx_data_sync.cli.run_reconciliation") as mock_backend:
        widget = ReconciliationWidget(repo)
        assert widget.txt_start_date is not None
        assert widget.txt_end_date is not None
        assert widget.spin_workers.value() == DEFAULT_RANGE_WORKERS
        assert widget.chk_force_recheck.isChecked() is False
        assert widget.table.columnCount() == 6
        mock_backend.assert_not_called()


def test_force_recheck_rejected_in_dry_run_mode(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = ReconciliationWidget(repo)
    widget.chk_force_recheck.setChecked(True)

    with patch("psx_data_sync.gui.reconciliation_panel.run_reconciliation") as mock_backend:
        widget.run_reconcile(apply=False)
        assert "Force Recheck can only be used with Apply mode" in widget.error_label.text()
        mock_backend.assert_not_called()


def test_date_validation_errors(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = ReconciliationWidget(repo)

    # Invalid start date
    widget.txt_start_date.setText("invalid-date")
    widget.txt_end_date.setText("2026-08-05")
    widget.run_reconcile(apply=False)
    assert "Invalid date format" in widget.error_label.text()

    # Start date after end date
    widget.txt_start_date.setText("2026-08-10")
    widget.txt_end_date.setText("2026-08-05")
    widget.run_reconcile(apply=False)
    assert "Start date cannot be after end date" in widget.error_label.text()


def test_large_range_confirmation_cancellation_aborts_reconciliation(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = ReconciliationWidget(repo)
    # Range > 90 days (100 days)
    widget.txt_start_date.setText("2026-01-01")
    widget.txt_end_date.setText("2026-04-10")

    with patch("psx_data_sync.gui.reconciliation_panel.run_reconciliation") as mock_backend:
        with patch.object(QMessageBox, "question", return_value=QMessageBox.StandardButton.No):
            widget.run_reconcile(apply=False)

        assert widget.lbl_status.text() == "Reconciliation cancelled by user."
        mock_backend.assert_not_called()


def test_apply_mode_confirmation_cancellation_aborts_action(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = ReconciliationWidget(repo)
    widget.txt_start_date.setText("2026-08-01")
    widget.txt_end_date.setText("2026-08-02")

    with patch("psx_data_sync.gui.reconciliation_panel.run_reconciliation") as mock_backend:
        with patch.object(QMessageBox, "question", return_value=QMessageBox.StandardButton.No):
            widget.run_reconcile(apply=True)

        assert widget.lbl_status.text() == "Reconciliation cancelled by user."
        mock_backend.assert_not_called()


def test_dry_run_reconciliation_execution(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = ReconciliationWidget(repo)
    widget.txt_start_date.setText("2026-08-01")
    widget.txt_end_date.setText("2026-08-02")

    dummy_res = _dummy_reconciliation_result(mode=ReconciliationMode.DRY_RUN)

    with patch("psx_data_sync.gui.reconciliation_panel.run_reconciliation", return_value=dummy_res) as mock_backend:
        widget.run_reconcile(apply=False)

        if widget.active_worker:
            widget.active_worker.wait(5000)
            qapp.processEvents()

        mock_backend.assert_called_once()
        call_kwargs = mock_backend.call_args.kwargs
        assert mock_backend.call_args.args == ("2026-08-01", "2026-08-02")
        assert call_kwargs["workers"] == 4
        assert call_kwargs["apply"] is False
        assert call_kwargs["force_recheck"] is False

    assert widget.last_result is not None
    assert widget.card_requested.value_label.text() == "2"
    assert widget.card_resolved.value_label.text() == "2"
    assert widget.card_sync_pct.value_label.text() == "100.0%"
    assert widget.table.rowCount() == 2


def test_apply_reconciliation_execution_and_callback(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    callback_mock = MagicMock()
    widget = ReconciliationWidget(repo, on_reconcile_success=callback_mock)
    widget.txt_start_date.setText("2026-08-01")
    widget.txt_end_date.setText("2026-08-02")
    widget.chk_force_recheck.setChecked(True)

    dummy_res = _dummy_reconciliation_result(mode=ReconciliationMode.APPLY)

    with patch("psx_data_sync.gui.reconciliation_panel.run_reconciliation", return_value=dummy_res) as mock_backend:
        with patch.object(QMessageBox, "question", return_value=QMessageBox.StandardButton.Yes):
            widget.run_reconcile(apply=True)

        if widget.active_worker:
            widget.active_worker.wait(5000)
            qapp.processEvents()

        mock_backend.assert_called_once()
        call_kwargs = mock_backend.call_args.kwargs
        assert mock_backend.call_args.args == ("2026-08-01", "2026-08-02")
        assert call_kwargs["workers"] == 4
        assert call_kwargs["apply"] is True
        assert call_kwargs["force_recheck"] is True

    assert widget.btn_apply.isEnabled() is True
    assert widget.progress_bar.isVisible() is False
    assert widget.active_worker is None
    callback_mock.assert_called_once()
