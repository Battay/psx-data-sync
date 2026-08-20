"""Reconciliation panel widget for PSX Data Sync desktop GUI."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date, timedelta
from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ..cli import run_reconciliation
from ..config import DEFAULT_RANGE_WORKERS, MAX_RANGE_WORKERS, MIN_RANGE_WORKERS, Settings
from ..downloader import validate_requested_date
from ..state import ReconciliationAction, ReconciliationMode, ReconciliationRangeResult
from .dashboard import MetricCard
from .workers import BaseWorker

if TYPE_CHECKING:
    from ..state_db import StateRepository

logger = logging.getLogger(__name__)


class ReconciliationWidget(QWidget):
    """GUI Panel for running PSX state reconciliation (Dry Run and Apply)."""

    def __init__(
        self,
        repository: StateRepository,
        on_reconcile_success: Callable[[], None] | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.repository = repository
        self.on_reconcile_success = on_reconcile_success
        self.last_result: ReconciliationRangeResult | None = None
        self.active_worker: BaseWorker | None = None

        self._init_ui()

    def _init_ui(self) -> None:
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(16, 16, 16, 16)
        main_layout.setSpacing(12)

        # Controls Group Box
        controls_group = QGroupBox("Reconciliation Configuration")
        controls_layout = QHBoxLayout(controls_group)
        controls_layout.setSpacing(12)

        today = date.today()
        default_start = (today - timedelta(days=30)).isoformat()
        default_end = today.isoformat()

        lbl_start = QLabel("Start Date:")
        lbl_start.setStyleSheet("font-weight: bold;")
        controls_layout.addWidget(lbl_start)

        self.txt_start_date = QLineEdit(default_start)
        self.txt_start_date.setPlaceholderText("YYYY-MM-DD")
        self.txt_start_date.setFixedWidth(110)
        controls_layout.addWidget(self.txt_start_date)

        lbl_end = QLabel("End Date:")
        lbl_end.setStyleSheet("font-weight: bold;")
        controls_layout.addWidget(lbl_end)

        self.txt_end_date = QLineEdit(default_end)
        self.txt_end_date.setPlaceholderText("YYYY-MM-DD")
        self.txt_end_date.setFixedWidth(110)
        controls_layout.addWidget(self.txt_end_date)

        lbl_workers = QLabel("Workers:")
        lbl_workers.setStyleSheet("font-weight: bold;")
        controls_layout.addWidget(lbl_workers)

        self.spin_workers = QSpinBox()
        self.spin_workers.setRange(MIN_RANGE_WORKERS, MAX_RANGE_WORKERS)
        self.spin_workers.setValue(DEFAULT_RANGE_WORKERS)
        self.spin_workers.setFixedWidth(70)
        controls_layout.addWidget(self.spin_workers)

        self.chk_force_recheck = QCheckBox("Force Recheck")
        self.chk_force_recheck.setToolTip("Allowed only when running in Apply mode.")
        controls_layout.addWidget(self.chk_force_recheck)

        controls_layout.addStretch()

        self.btn_dry_run = QPushButton("Dry Run (Plan Only)")
        self.btn_dry_run.setStyleSheet("font-weight: bold;")
        self.btn_dry_run.clicked.connect(lambda: self.run_reconcile(apply=False))
        controls_layout.addWidget(self.btn_dry_run)

        self.btn_apply = QPushButton("Apply Reconciliation")
        self.btn_apply.setStyleSheet(
            "font-weight: bold; background-color: #0066cc; color: white; padding: 6px 16px;"
        )
        self.btn_apply.clicked.connect(lambda: self.run_reconcile(apply=True))
        controls_layout.addWidget(self.btn_apply)

        main_layout.addWidget(controls_group)

        # Status & Progress Display Bar
        status_layout = QHBoxLayout()
        self.lbl_status = QLabel("Ready. Specify a date range for reconciliation.")
        self.lbl_status.setStyleSheet("font-size: 13px; color: #444;")
        status_layout.addWidget(self.lbl_status)

        status_layout.addStretch()

        self.error_label = QLabel("")
        self.error_label.setStyleSheet("color: red; font-weight: bold;")
        self.error_label.setVisible(False)
        status_layout.addWidget(self.error_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setFixedWidth(150)
        self.progress_bar.setVisible(False)
        status_layout.addWidget(self.progress_bar)

        main_layout.addLayout(status_layout)

        # Summary Metrics Box
        summary_group = QGroupBox("Reconciliation Summary")
        summary_grid = QGridLayout(summary_group)
        summary_grid.setSpacing(10)

        self.card_requested = MetricCard("Requested Dates", "—")
        self.card_resolved = MetricCard("Healthy / Resolved", "—")
        self.card_sync_pct = MetricCard("Synchronized %", "—")
        self.card_rechecks = MetricCard("Network Rechecks", "—")
        self.card_reindex = MetricCard("Local Reindex", "—")
        self.card_repairs = MetricCard("Repairs", "—")
        self.card_unresolved = MetricCard("Unresolved", "—")
        self.card_manual_review = MetricCard("Manual Review", "—")
        self.card_file_issues = MetricCard("File Health Issues", "—")
        self.card_failures = MetricCard("Failures", "—")
        self.card_duration = MetricCard("Duration", "—")

        summary_grid.addWidget(self.card_requested, 0, 0)
        summary_grid.addWidget(self.card_resolved, 0, 1)
        summary_grid.addWidget(self.card_sync_pct, 0, 2)
        summary_grid.addWidget(self.card_rechecks, 0, 3)
        summary_grid.addWidget(self.card_reindex, 0, 4)

        summary_grid.addWidget(self.card_repairs, 1, 0)
        summary_grid.addWidget(self.card_unresolved, 1, 1)
        summary_grid.addWidget(self.card_manual_review, 1, 2)
        summary_grid.addWidget(self.card_file_issues, 1, 3)
        summary_grid.addWidget(self.card_failures, 1, 4)

        main_layout.addWidget(summary_group)

        # Per-Date Decision Table
        table_group = QGroupBox("Per-Date Decision & Evidence Details")
        table_layout = QVBoxLayout(table_group)
        table_layout.setContentsMargins(8, 8, 8, 8)

        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels([
            "Market Date",
            "Current Status",
            "Planned Action",
            "Resulting Status",
            "Network / Attempts",
            "Details / Reasons / Warnings",
        ])
        header = self.table.horizontalHeader()
        if header is not None:
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        self.table.setSortingEnabled(True)
        table_layout.addWidget(self.table)

        main_layout.addWidget(table_group)

    def _set_controls_enabled(self, enabled: bool) -> None:
        self.txt_start_date.setEnabled(enabled)
        self.txt_end_date.setEnabled(enabled)
        self.spin_workers.setEnabled(enabled)
        self.chk_force_recheck.setEnabled(enabled)
        self.btn_dry_run.setEnabled(enabled)
        self.btn_apply.setEnabled(enabled)

    def _show_error(self, message: str) -> None:
        self.error_label.setText(message)
        self.error_label.setVisible(True)

    def run_reconcile(self, apply: bool = False) -> None:
        """Execute reconciliation in dry-run or apply mode with safety guards."""

        if self.active_worker is not None and self.active_worker.isRunning():
            return

        force_recheck = self.chk_force_recheck.isChecked()
        if force_recheck and not apply:
            self._show_error("Force Recheck can only be used with Apply mode.")
            return

        start_str = self.txt_start_date.text().strip()
        end_str = self.txt_end_date.text().strip()
        workers = self.spin_workers.value()

        try:
            d_start = validate_requested_date(start_str)
            d_end = validate_requested_date(end_str)
        except Exception as exc:
            self._show_error(f"Invalid date format: {exc}")
            return

        if d_start > d_end:
            self._show_error("Start date cannot be after end date.")
            return

        cal_days = (d_end - d_start).days + 1
        if cal_days > 90:
            confirm = QMessageBox.question(
                self,
                "Large Range Reconciliation Warning",
                f"The requested date range spans {cal_days} calendar days (> 90 days).\n\n"
                "Do you want to proceed with reconciliation for this range?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if confirm != QMessageBox.StandardButton.Yes:
                self.lbl_status.setText("Reconciliation cancelled by user.")
                return

        if apply:
            confirm_apply = QMessageBox.question(
                self,
                "Confirm Reconciliation Apply",
                f"Are you sure you want to run reconciliation in Apply mode for:\n{start_str} to {end_str}?\n\n"
                "This may execute network rechecks, stage missing file repairs, and apply status transitions.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if confirm_apply != QMessageBox.StandardButton.Yes:
                self.lbl_status.setText("Reconciliation cancelled by user.")
                return

        self.error_label.setVisible(False)
        mode_label = "Apply" if apply else "Dry Run"
        self.lbl_status.setText(
            f"Running {mode_label} reconciliation on {cal_days} market dates (workers: {workers})..."
        )
        self.progress_bar.setVisible(True)
        self._set_controls_enabled(False)

        settings = Settings(
            state_db_path=self.repository.database_path,
            raw_output_dir=self.repository.project_root / "data" / "raw",
        )

        worker = BaseWorker(
            run_reconciliation,
            start_str,
            end_str,
            workers=workers,
            apply=apply,
            force_recheck=force_recheck,
            settings=settings,
        )
        worker.signals.result.connect(self._on_reconcile_completed)
        worker.signals.error.connect(self._on_reconcile_error)
        worker.signals.finished.connect(self._on_worker_finished)
        worker.finished.connect(worker.deleteLater)

        self.active_worker = worker
        worker.start()

    def _on_reconcile_completed(self, result: ReconciliationRangeResult) -> None:
        try:
            self.last_result = result

            healthy_resolved = (
                result.verified_count + result.confirmed_non_trading_count
            )
            rechecks_cnt = (
                result.network_recheck_count
                if result.mode is ReconciliationMode.APPLY
                else result.network_recheck_planned_count
            )
            reindex_cnt = result.counts_by_action.get(
                ReconciliationAction.LOCAL_REINDEX, 0
            )

            # Update Metric Cards
            self.card_requested.set_value(f"{len(result.requested_dates):,}")
            self.card_resolved.set_value(f"{healthy_resolved:,}")
            self.card_sync_pct.set_value(f"{result.resolution_percentage:.1f}%")
            self.card_rechecks.set_value(f"{rechecks_cnt:,}")
            self.card_reindex.set_value(f"{reindex_cnt:,}")
            self.card_repairs.set_value(f"{result.local_repair_count:,}")
            self.card_unresolved.set_value(f"{result.unresolved_count:,}")
            self.card_manual_review.set_value(f"{result.manual_review_count:,}")
            self.card_file_issues.set_value(f"{result.file_health_issue_count:,}")
            self.card_failures.set_value(f"{result.failure_count:,}")
            self.card_duration.set_value(f"{result.duration_ms / 1000.0:.2f} s")

            # Populate Per-Date Decision Table
            self.table.setSortingEnabled(False)
            self.table.setRowCount(len(result.results))

            for row_idx, r in enumerate(result.results):
                prev_status_str = (
                    r.previous_status.value
                    if hasattr(r.previous_status, "value")
                    else str(r.previous_status)
                )
                reconciled_status_str = (
                    r.reconciled_status.value
                    if hasattr(r.reconciled_status, "value")
                    else str(r.reconciled_status)
                )
                action_str = (
                    r.action_required.value
                    if hasattr(r.action_required, "value")
                    else str(r.action_required)
                )

                attempts_str = f"Attempts: {r.attempt_count}"
                if r.network_recheck_required:
                    attempts_str += " (recheck)"

                item_date = QTableWidgetItem(r.market_date)
                item_prev = QTableWidgetItem(prev_status_str)
                item_action = QTableWidgetItem(action_str)
                item_rec = QTableWidgetItem(reconciled_status_str)
                item_attempts = QTableWidgetItem(attempts_str)

                details_parts: list[str] = []
                if r.reasons:
                    details_parts.append(f"Reasons: {'; '.join(r.reasons)}")
                if r.warnings:
                    details_parts.append(f"Warnings: {'; '.join(r.warnings)}")
                if not details_parts:
                    details_parts.append("OK")
                item_details = QTableWidgetItem(" | ".join(details_parts))

                item_attempts.setTextAlignment(
                    Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
                )

                self.table.setItem(row_idx, 0, item_date)
                self.table.setItem(row_idx, 1, item_prev)
                self.table.setItem(row_idx, 2, item_action)
                self.table.setItem(row_idx, 3, item_rec)
                self.table.setItem(row_idx, 4, item_attempts)
                self.table.setItem(row_idx, 5, item_details)

            self.table.setSortingEnabled(True)

            mode_str = (
                "Apply" if result.mode is ReconciliationMode.APPLY else "Dry Run"
            )
            self.lbl_status.setText(
                f"Reconciliation ({mode_str}) finished in {result.duration_ms / 1000.0:.2f} s. "
                f"Synchronized: {result.resolution_percentage:.1f}%, Resolved: {healthy_resolved}, "
                f"Rechecks: {rechecks_cnt}, Unresolved: {result.unresolved_count}."
            )

            if (
                result.mode is ReconciliationMode.APPLY
                and self.on_reconcile_success
            ):
                try:
                    self.on_reconcile_success()
                except Exception as exc:
                    logger.exception("failed to trigger on_reconcile_success callback")
        except Exception as exc:
            logger.exception("error rendering reconciliation results")
            self._show_error(f"Error rendering reconciliation results: {exc}")
            self.lbl_status.setText("Reconciliation results rendering failed.")

    def _on_reconcile_error(self, error_msg: str) -> None:
        self._show_error(f"Reconciliation process failed: {error_msg}")
        self.lbl_status.setText("Reconciliation failed due to error.")

    def _on_worker_finished(self) -> None:
        self.progress_bar.setVisible(False)
        self._set_controls_enabled(True)
        self.active_worker = None
