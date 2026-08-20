"""Parquet export panel widget for PSX Data Sync desktop GUI."""

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
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from ..downloader import validate_requested_date
from ..parquet_sync import RangeParquetSyncResult, sync_parquet_range
from .dashboard import MetricCard
from .workers import BaseWorker

if TYPE_CHECKING:
    from ..state_db import StateRepository

logger = logging.getLogger(__name__)


class ParquetExportWidget(QWidget):
    """GUI Panel for running Parquet export and synchronization."""

    def __init__(
        self,
        repository: StateRepository,
        on_export_success: Callable[[], None] | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.repository = repository
        self.on_export_success = on_export_success
        self.last_result: RangeParquetSyncResult | None = None
        self.active_worker: BaseWorker | None = None

        self._init_ui()

    def _init_ui(self) -> None:
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(16, 16, 16, 16)
        main_layout.setSpacing(12)

        # Controls Group Box
        controls_group = QGroupBox("Parquet Export Configuration")
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

        self.chk_rebuild = QCheckBox("Rebuild (Force Overwrite)")
        self.chk_rebuild.setToolTip("Allowed only when running in Apply mode.")
        controls_layout.addWidget(self.chk_rebuild)

        controls_layout.addStretch()

        self.btn_dry_run = QPushButton("Dry Run (Plan Only)")
        self.btn_dry_run.clicked.connect(lambda: self.run_export(dry_run=True))
        controls_layout.addWidget(self.btn_dry_run)

        self.btn_apply = QPushButton("Apply Parquet Export")
        self.btn_apply.setProperty("accent", True)
        self.btn_apply.clicked.connect(lambda: self.run_export(dry_run=False))
        controls_layout.addWidget(self.btn_apply)

        main_layout.addWidget(controls_group)

        # Status & Progress Display Bar
        status_layout = QHBoxLayout()
        self.lbl_status = QLabel("Ready. Specify a date range for Parquet export.")
        self.lbl_status.setStyleSheet("font-size: 13px; color: #94a3b8;")
        status_layout.addWidget(self.lbl_status)

        status_layout.addStretch()

        self.error_label = QLabel("")
        self.error_label.setStyleSheet("color: #ef4444; font-weight: bold;")
        self.error_label.setVisible(False)
        status_layout.addWidget(self.error_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setFixedWidth(150)
        self.progress_bar.setVisible(False)
        status_layout.addWidget(self.progress_bar)

        main_layout.addLayout(status_layout)

        # Summary Metrics Box
        summary_group = QGroupBox("Parquet Export Summary")
        summary_grid = QGridLayout(summary_group)
        summary_grid.setSpacing(10)

        self.card_requested = MetricCard("Requested Dates", "—")
        self.card_eligible = MetricCard("Eligible", "—")
        self.card_sync_pct = MetricCard("Synchronized %", "—")
        self.card_current = MetricCard("Current / No-Op", "—")
        self.card_create = MetricCard("Create", "—")
        self.card_stale = MetricCard("Stale", "—")
        self.card_corrupt = MetricCard("Corrupt", "—")
        self.card_reindex = MetricCard("Reindex", "—")
        self.card_unresolved = MetricCard("Excluded Unresolved", "—")
        self.card_issues = MetricCard("Excluded Issues", "—")
        self.card_source_invalid = MetricCard("Source Invalid", "—")
        self.card_failed = MetricCard("Failed", "—")
        self.card_written = MetricCard("Written / Rebuilt", "—")
        self.card_duration = MetricCard("Duration", "—")

        summary_grid.addWidget(self.card_requested, 0, 0)
        summary_grid.addWidget(self.card_eligible, 0, 1)
        summary_grid.addWidget(self.card_sync_pct, 0, 2)
        summary_grid.addWidget(self.card_current, 0, 3)
        summary_grid.addWidget(self.card_create, 0, 4)

        summary_grid.addWidget(self.card_stale, 1, 0)
        summary_grid.addWidget(self.card_corrupt, 1, 1)
        summary_grid.addWidget(self.card_reindex, 1, 2)
        summary_grid.addWidget(self.card_unresolved, 1, 3)
        summary_grid.addWidget(self.card_issues, 1, 4)

        summary_grid.addWidget(self.card_source_invalid, 2, 0)
        summary_grid.addWidget(self.card_failed, 2, 1)
        summary_grid.addWidget(self.card_written, 2, 2)
        summary_grid.addWidget(self.card_duration, 2, 3)

        main_layout.addWidget(summary_group)

        # Per-Date Result Table
        table_group = QGroupBox("Per-Date Partition Sync Details")
        table_layout = QVBoxLayout(table_group)
        table_layout.setContentsMargins(8, 8, 8, 8)

        self.table = QTableWidget()
        self.table.setColumnCount(7)
        self.table.setAlternatingRowColors(True)
        v_header = self.table.verticalHeader()
        if v_header is not None:
            v_header.setDefaultSectionSize(30)
            v_header.setVisible(False)

        self.table.setHorizontalHeaderLabels([
            "Market Date",
            "Source Status",
            "Planned Action",
            "Parquet Before",
            "Parquet After",
            "Row Count",
            "Details / Warnings / Error",
        ])
        header = self.table.horizontalHeader()
        if header is not None:
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(6, QHeaderView.ResizeMode.Stretch)
        self.table.setSortingEnabled(True)
        table_layout.addWidget(self.table)

        main_layout.addWidget(table_group)

    def _set_controls_enabled(self, enabled: bool) -> None:
        self.txt_start_date.setEnabled(enabled)
        self.txt_end_date.setEnabled(enabled)
        self.chk_rebuild.setEnabled(enabled)
        self.btn_dry_run.setEnabled(enabled)
        self.btn_apply.setEnabled(enabled)

    def _show_error(self, message: str) -> None:
        self.error_label.setText(message)
        self.error_label.setVisible(True)

    def run_export(self, dry_run: bool = True) -> None:
        """Execute Parquet export in dry-run or apply mode with safety guards."""

        if self.active_worker is not None and self.active_worker.isRunning():
            return

        rebuild = self.chk_rebuild.isChecked()
        if rebuild and dry_run:
            self._show_error("Rebuild can only be used with Apply mode.")
            return

        start_str = self.txt_start_date.text().strip()
        end_str = self.txt_end_date.text().strip()

        try:
            d_start = validate_requested_date(start_str, today=date.max)
            d_end = validate_requested_date(end_str, today=date.max)
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
                "Large Range Parquet Export Warning",
                f"The requested date range spans {cal_days} calendar days (> 90 days).\n\n"
                "Do you want to proceed with Parquet export for this range?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if confirm != QMessageBox.StandardButton.Yes:
                self.lbl_status.setText("Parquet export cancelled by user.")
                return

        if not dry_run:
            confirm_apply = QMessageBox.question(
                self,
                "Confirm Parquet Export Apply",
                f"Are you sure you want to apply Parquet export for:\n{start_str} to {end_str}?\n\n"
                "This will write or rebuild derived Parquet partitions on disk.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if confirm_apply != QMessageBox.StandardButton.Yes:
                self.lbl_status.setText("Parquet export cancelled by user.")
                return

        self.error_label.setVisible(False)
        mode_label = "Dry Run" if dry_run else "Apply"
        self.lbl_status.setText(
            f"Running {mode_label} Parquet export on {cal_days} market dates..."
        )
        self.progress_bar.setVisible(True)
        self._set_controls_enabled(False)

        worker = BaseWorker(
            sync_parquet_range,
            self.repository,
            start_str,
            end_str,
            dry_run=dry_run,
            rebuild=rebuild,
        )
        worker.signals.result.connect(self._on_export_completed)
        worker.signals.error.connect(self._on_export_error)
        worker.signals.finished.connect(self._on_worker_finished)
        worker.finished.connect(worker.deleteLater)

        self.active_worker = worker
        worker.start()

    def _on_export_completed(self, result: RangeParquetSyncResult) -> None:
        try:
            self.last_result = result

            excluded_issues = (
                result.excluded_failure_count + result.excluded_file_issue_count
            )

            # Update Metric Cards
            self.card_requested.set_value(f"{result.requested_count:,}")
            self.card_eligible.set_value(f"{result.eligible_count:,}")
            self.card_sync_pct.set_value(f"{result.synchronization_percentage:.1f}%")
            self.card_current.set_value(f"{result.current_count:,}")
            self.card_create.set_value(f"{result.create_count:,}")
            self.card_stale.set_value(f"{result.stale_count:,}")
            self.card_corrupt.set_value(f"{result.corrupt_count:,}")
            self.card_reindex.set_value(f"{result.reindexed_count:,}")
            self.card_unresolved.set_value(f"{result.excluded_unresolved_count:,}")
            self.card_issues.set_value(f"{excluded_issues:,}")
            self.card_source_invalid.set_value(f"{result.source_invalid_count:,}")
            self.card_failed.set_value(f"{result.failed_count:,}")
            self.card_written.set_value(f"{result.written_or_rebuilt_count:,}")
            self.card_duration.set_value(f"{result.duration_ms / 1000.0:.2f} s")

            # Populate Per-Date Decision Table
            self.table.setSortingEnabled(False)
            self.table.setRowCount(len(result.results))

            for row_idx, r in enumerate(result.results):
                source_str = (
                    r.source_status.value
                    if r.source_status and hasattr(r.source_status, "value")
                    else str(r.source_status or "—")
                )
                action_str = (
                    r.action.value if hasattr(r.action, "value") else str(r.action)
                )

                before_str = (
                    r.export_status_before.value
                    if r.export_status_before and hasattr(r.export_status_before, "value")
                    else str(r.export_status_before or "—")
                )

                target_status = (
                    r.export_status_planned if r.dry_run else r.export_status_after
                )
                after_str = (
                    target_status.value
                    if target_status and hasattr(target_status, "value")
                    else str(target_status or "—")
                )

                row_cnt_str = (
                    f"{r.parquet_row_count:,}"
                    if r.parquet_row_count is not None
                    else (f"{r.source_row_count:,}" if r.source_row_count is not None else "—")
                )

                item_date = QTableWidgetItem(r.market_date)
                item_src = QTableWidgetItem(source_str)
                item_act = QTableWidgetItem(action_str)
                item_bef = QTableWidgetItem(before_str)
                item_aft = QTableWidgetItem(after_str)
                item_rows = QTableWidgetItem(row_cnt_str)

                details_parts: list[str] = []
                if r.error:
                    details_parts.append(f"Error: {r.error}")
                if r.warnings:
                    details_parts.append(f"Warnings: {'; '.join(r.warnings)}")
                if not details_parts:
                    details_parts.append("OK")
                item_details = QTableWidgetItem(" | ".join(details_parts))

                item_rows.setTextAlignment(
                    Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
                )

                self.table.setItem(row_idx, 0, item_date)
                self.table.setItem(row_idx, 1, item_src)
                self.table.setItem(row_idx, 2, item_act)
                self.table.setItem(row_idx, 3, item_bef)
                self.table.setItem(row_idx, 4, item_aft)
                self.table.setItem(row_idx, 5, item_rows)
                self.table.setItem(row_idx, 6, item_details)

            self.table.setSortingEnabled(True)

            mode_str = "Dry Run" if result.dry_run else "Apply"
            self.lbl_status.setText(
                f"Parquet export ({mode_str}) finished in {result.duration_ms / 1000.0:.2f} s. "
                f"Synchronized: {result.synchronization_percentage:.1f}%, Eligible: {result.eligible_count}, "
                f"Written/Rebuilt: {result.written_or_rebuilt_count}."
            )

            if not result.dry_run and self.on_export_success:
                try:
                    self.on_export_success()
                except Exception as exc:
                    logger.exception("failed to trigger on_export_success callback")
        except Exception as exc:
            logger.exception("error rendering parquet export results")
            self._show_error(f"Error rendering parquet export results: {exc}")
            self.lbl_status.setText("Parquet export results rendering failed.")

    def _on_export_error(self, error_msg: str) -> None:
        self._show_error(f"Parquet export failed: {error_msg}")
        self.lbl_status.setText("Parquet export failed due to error.")

    def _on_worker_finished(self) -> None:
        self.progress_bar.setVisible(False)
        self._set_controls_enabled(True)
        self.active_worker = None
