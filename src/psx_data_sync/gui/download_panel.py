"""Incremental range download panel widget for PSX Data Sync desktop GUI."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date, timedelta
from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
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

from ..cli import run_range_download
from ..config import DEFAULT_RANGE_WORKERS, MAX_RANGE_WORKERS, MIN_RANGE_WORKERS, Settings
from ..downloader import validate_requested_date
from ..state import RangeDownloadResult
from .dashboard import MetricCard
from .widgets import PSXDateEdit
from .workers import BaseWorker

if TYPE_CHECKING:
    from ..state_db import StateRepository

logger = logging.getLogger(__name__)


class DownloadWidget(QWidget):
    """GUI Panel for running incremental PSX date-range downloads."""

    def __init__(
        self,
        repository: StateRepository,
        on_download_success: Callable[[], None] | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.repository = repository
        self.on_download_success = on_download_success
        self.last_result: RangeDownloadResult | None = None
        self.active_worker: BaseWorker | None = None

        self._init_ui()

    def _init_ui(self) -> None:
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(16, 16, 16, 16)
        main_layout.setSpacing(12)

        # Controls Box
        controls_group = QGroupBox("Incremental Range Download Configuration")
        controls_layout = QHBoxLayout(controls_group)
        controls_layout.setSpacing(12)

        today = date.today()
        default_start = (today - timedelta(days=7)).isoformat()
        default_end = today.isoformat()

        lbl_start = QLabel("Start Date:")
        lbl_start.setStyleSheet("font-weight: bold;")
        controls_layout.addWidget(lbl_start)

        self.txt_start_date = PSXDateEdit(default_start)
        controls_layout.addWidget(self.txt_start_date)

        lbl_end = QLabel("End Date:")
        lbl_end.setStyleSheet("font-weight: bold;")
        controls_layout.addWidget(lbl_end)

        self.txt_end_date = PSXDateEdit(default_end)
        controls_layout.addWidget(self.txt_end_date)

        lbl_workers = QLabel("Workers:")
        lbl_workers.setStyleSheet("font-weight: bold;")
        controls_layout.addWidget(lbl_workers)

        self.spin_workers = QSpinBox()
        self.spin_workers.setRange(MIN_RANGE_WORKERS, MAX_RANGE_WORKERS)
        self.spin_workers.setValue(DEFAULT_RANGE_WORKERS)
        self.spin_workers.setFixedWidth(70)
        controls_layout.addWidget(self.spin_workers)

        controls_layout.addStretch()

        self.btn_download = QPushButton("Download / Fetch Range")
        self.btn_download.setProperty("accent", True)
        self.btn_download.clicked.connect(self.run_download)
        controls_layout.addWidget(self.btn_download)

        main_layout.addWidget(controls_group)

        # Status & Progress Bar
        status_layout = QHBoxLayout()
        self.lbl_status = QLabel("Ready. Specify a date range to fetch.")
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
        summary_group = QGroupBox("Range Download Summary")
        summary_grid = QGridLayout(summary_group)
        summary_grid.setSpacing(10)

        self.card_requested = MetricCard("Requested Dates", "—")
        self.card_verified = MetricCard("Verified Successful", "—")
        self.card_skipped = MetricCard("Locally Skipped", "—")
        self.card_unresolved = MetricCard("Unresolved / Empty", "—")
        self.card_failures = MetricCard("Failures", "—")
        self.card_total_rows = MetricCard("Total Valid Rows", "—")
        self.card_duration = MetricCard("Duration", "—")

        summary_grid.addWidget(self.card_requested, 0, 0)
        summary_grid.addWidget(self.card_verified, 0, 1)
        summary_grid.addWidget(self.card_skipped, 0, 2)
        summary_grid.addWidget(self.card_unresolved, 0, 3)

        summary_grid.addWidget(self.card_failures, 1, 0)
        summary_grid.addWidget(self.card_total_rows, 1, 1)
        summary_grid.addWidget(self.card_duration, 1, 2)

        main_layout.addWidget(summary_group)

        # Per-Date Detailed Result Table
        table_group = QGroupBox("Per-Date Download & Preflight Details")
        table_layout = QVBoxLayout(table_group)
        table_layout.setContentsMargins(8, 8, 8, 8)

        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setAlternatingRowColors(True)
        v_header = self.table.verticalHeader()
        if v_header is not None:
            v_header.setDefaultSectionSize(30)
            v_header.setVisible(False)
        self.table.setHorizontalHeaderLabels([
            "Market Date",
            "Outcome / Status",
            "Valid Rows",
            "HTTP Status",
            "Attempts",
            "Details / Warning / Error",
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
        self.btn_download.setEnabled(enabled)

    def _show_error(self, message: str) -> None:
        self.error_label.setText(message)
        self.error_label.setVisible(True)

    def run_download(self) -> None:
        """Execute range download via background worker with safety validations."""

        if self.active_worker is not None and self.active_worker.isRunning():
            return

        start_str = self.txt_start_date.date_str
        end_str = self.txt_end_date.date_str
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
                "Large Range Warning",
                f"The requested date range spans {cal_days} calendar days (> 90 days).\n\n"
                "Do you want to proceed with downloading this range?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if confirm != QMessageBox.StandardButton.Yes:
                self.lbl_status.setText("Range download cancelled by user.")
                return

        self.error_label.setVisible(False)
        self.lbl_status.setText(
            f"Downloading {cal_days} market dates from {start_str} to {end_str} (workers: {workers})..."
        )
        self.progress_bar.setVisible(True)
        self._set_controls_enabled(False)

        settings = Settings(
            state_db_path=self.repository.database_path,
            raw_output_dir=self.repository.raw_output_dir,
        )

        worker = BaseWorker(
            run_range_download,
            start_str,
            end_str,
            workers=workers,
            settings=settings,
        )
        worker.signals.result.connect(self._on_download_completed)
        worker.signals.error.connect(self._on_download_error)
        worker.signals.finished.connect(self._on_worker_finished)
        worker.finished.connect(worker.deleteLater)

        self.active_worker = worker
        worker.start()

    def _on_download_completed(self, result: RangeDownloadResult) -> None:
        try:
            self.last_result = result

            # Update Metric Cards
            self.card_requested.set_value(f"{len(result.requested_dates):,}")
            self.card_verified.set_value(f"{result.verified_successful_dates:,}")
            self.card_skipped.set_value(f"{result.locally_skipped_dates:,}")
            self.card_unresolved.set_value(f"{len(result.unresolved_empty_dates):,}")
            self.card_failures.set_value(f"{len(result.failed_dates):,}")
            self.card_total_rows.set_value(f"{result.total_valid_rows:,}")
            self.card_duration.set_value(f"{result.total_duration_ms / 1000.0:.2f} s")

            # Populate Per-Date Table
            self.table.setSortingEnabled(False)
            self.table.setRowCount(len(result.results))

            for row_idx, r in enumerate(result.results):
                status_str = (
                    r.status.value if hasattr(r.status, "value") else str(r.status)
                )
                if r.locally_skipped:
                    status_str = f"LOCAL_SKIP ({status_str})"

                valid_cnt_str = (
                    f"{r.valid_row_count:,}" if r.valid_row_count is not None else "0"
                )
                http_str = str(r.http_status) if r.http_status is not None else "—"
                attempts_str = str(r.attempts) if r.attempts is not None else "1"

                item_date = QTableWidgetItem(r.requested_date)
                item_status = QTableWidgetItem(status_str)
                item_valid = QTableWidgetItem(valid_cnt_str)
                item_http = QTableWidgetItem(http_str)
                item_attempts = QTableWidgetItem(attempts_str)

                details_parts: list[str] = []
                if r.error:
                    details_parts.append(f"Error: {r.error}")
                if r.warnings:
                    details_parts.append(f"Warnings: {'; '.join(r.warnings)}")
                if not details_parts:
                    details_parts.append("OK")
                item_details = QTableWidgetItem(" | ".join(details_parts))

                # Alignments
                item_valid.setTextAlignment(
                    Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
                )
                item_http.setTextAlignment(
                    Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
                )
                item_attempts.setTextAlignment(
                    Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
                )

                self.table.setItem(row_idx, 0, item_date)
                self.table.setItem(row_idx, 1, item_status)
                self.table.setItem(row_idx, 2, item_valid)
                self.table.setItem(row_idx, 3, item_http)
                self.table.setItem(row_idx, 4, item_attempts)
                self.table.setItem(row_idx, 5, item_details)

            self.table.setSortingEnabled(True)

            self.lbl_status.setText(
                f"Range download finished in {result.total_duration_ms / 1000.0:.2f} s. "
                f"Verified: {result.verified_successful_dates}, Locally Skipped: {result.locally_skipped_dates}, "
                f"Unresolved: {len(result.unresolved_empty_dates)}, Failures: {len(result.failed_dates)}."
            )

            if self.on_download_success:
                try:
                    self.on_download_success()
                except Exception as exc:
                    logger.exception("failed to trigger on_download_success callback")
        except Exception as exc:
            logger.exception("error rendering download results")
            self._show_error(f"Error rendering download results: {exc}")
            self.lbl_status.setText("Download results rendering failed.")

    def _on_download_error(self, error_msg: str) -> None:
        self._show_error(f"Range download failed: {error_msg}")
        self.lbl_status.setText("Download failed due to error.")

    def _on_worker_finished(self) -> None:
        self.progress_bar.setVisible(False)
        self._set_controls_enabled(True)
        self.active_worker = None
