"""Consolidated Parquet export panel for the desktop GUI."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from PySide6.QtWidgets import (
    QCheckBox,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from ..parquet_sync import ConsolidatedParquetSyncResult, sync_consolidated_parquet
from .dashboard import MetricCard
from .workers import BaseWorker

if TYPE_CHECKING:
    from ..state_db import StateRepository

logger = logging.getLogger(__name__)


class ParquetExportWidget(QWidget):
    """Plan and build one Parquet file from the complete verified CSV set."""

    def __init__(
        self,
        repository: StateRepository,
        on_export_success: Callable[[], None] | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.repository = repository
        self.on_export_success = on_export_success
        self.last_result: ConsolidatedParquetSyncResult | None = None
        self.active_worker: BaseWorker | None = None
        self._init_ui()

    def _init_ui(self) -> None:
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(16, 16, 16, 16)
        main_layout.setSpacing(12)

        controls_group = QGroupBox("Consolidated Parquet Export")
        controls_layout = QHBoxLayout(controls_group)
        description = QLabel(
            "Build data/parquet/market.parquet from every verified canonical CSV. "
            "No network access is used."
        )
        description.setWordWrap(True)
        controls_layout.addWidget(description, 1)
        self.chk_rebuild = QCheckBox("Rebuild current file")
        self.chk_rebuild.setToolTip("Force a full rebuild; available only in Apply mode.")
        controls_layout.addWidget(self.chk_rebuild)
        self.btn_dry_run = QPushButton("Dry Run (Plan Only)")
        self.btn_dry_run.clicked.connect(lambda: self.run_export(dry_run=True))
        controls_layout.addWidget(self.btn_dry_run)
        self.btn_apply = QPushButton("Apply Consolidated Export")
        self.btn_apply.setProperty("accent", True)
        self.btn_apply.clicked.connect(lambda: self.run_export(dry_run=False))
        controls_layout.addWidget(self.btn_apply)
        main_layout.addWidget(controls_group)

        status_layout = QHBoxLayout()
        self.lbl_status = QLabel("Ready. Dry-run inspects the complete verified local dataset.")
        self.lbl_status.setStyleSheet("font-size: 13px; color: #94a3b8;")
        status_layout.addWidget(self.lbl_status, 1)
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

        summary_group = QGroupBox("Consolidated Dataset Summary")
        summary_grid = QGridLayout(summary_group)
        summary_grid.setSpacing(10)
        self.card_source_dates = MetricCard("Verified Source Dates", "—")
        self.card_source_rows = MetricCard("Total Source Rows", "—")
        self.card_status = MetricCard("Dataset Status", "—")
        self.card_rows_written = MetricCard("Rows Written", "—")
        self.card_file_size = MetricCard("File Size", "—")
        self.card_last_build = MetricCard("Last Build", "—")
        self.card_legacy = MetricCard("Legacy Partitions", "—")
        self.card_action = MetricCard("Action", "—")
        self.card_duration = MetricCard("Duration", "—")
        for index, card in enumerate(
            (
                self.card_source_dates,
                self.card_source_rows,
                self.card_status,
                self.card_rows_written,
                self.card_file_size,
                self.card_last_build,
                self.card_legacy,
                self.card_action,
                self.card_duration,
            )
        ):
            summary_grid.addWidget(card, index // 3, index % 3)
        main_layout.addWidget(summary_group)

        details_group = QGroupBox("Dataset Identity and Output")
        details_grid = QGridLayout(details_group)
        details_grid.addWidget(QLabel("Output path:"), 0, 0)
        self.txt_output_path = QLineEdit()
        self.txt_output_path.setReadOnly(True)
        details_grid.addWidget(self.txt_output_path, 0, 1)
        details_grid.addWidget(QLabel("Source identity:"), 1, 0)
        self.txt_source_identity = QLineEdit()
        self.txt_source_identity.setReadOnly(True)
        details_grid.addWidget(self.txt_source_identity, 1, 1)
        main_layout.addWidget(details_group)
        main_layout.addStretch()

    def _set_controls_enabled(self, enabled: bool) -> None:
        self.chk_rebuild.setEnabled(enabled)
        self.btn_dry_run.setEnabled(enabled)
        self.btn_apply.setEnabled(enabled)

    def _show_error(self, message: str) -> None:
        self.error_label.setText(message)
        self.error_label.setVisible(True)

    def run_export(self, dry_run: bool = True) -> None:
        """Run full-source inspection or confirmed consolidated build."""

        if self.active_worker is not None and self.active_worker.isRunning():
            return
        rebuild = self.chk_rebuild.isChecked()
        if rebuild and dry_run:
            self._show_error("Rebuild can only be used with Apply mode.")
            return
        if not dry_run:
            answer = QMessageBox.question(
                self,
                "Confirm Consolidated Parquet Export",
                "Build one consolidated Parquet file from every verified canonical CSV?\n\n"
                "The existing consolidated file will be replaced atomically. Legacy "
                "partition folders will not be deleted.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if answer != QMessageBox.StandardButton.Yes:
                self.lbl_status.setText("Parquet export cancelled by user.")
                return

        self.error_label.setVisible(False)
        self.lbl_status.setText(
            "Inspecting complete verified source set..."
            if dry_run
            else "Building consolidated Parquet file..."
        )
        self.progress_bar.setVisible(True)
        self._set_controls_enabled(False)
        worker = BaseWorker(
            sync_consolidated_parquet,
            self.repository,
            output_root=self.repository.raw_output_dir.parent / "parquet",
            dry_run=dry_run,
            rebuild=rebuild,
        )
        worker.signals.result.connect(self._on_export_completed)
        worker.signals.error.connect(self._on_export_error)
        worker.signals.finished.connect(self._on_worker_finished)
        worker.finished.connect(worker.deleteLater)
        self.active_worker = worker
        worker.start()

    def _on_export_completed(self, result: ConsolidatedParquetSyncResult) -> None:
        try:
            self.last_result = result
            self.card_source_dates.set_value(f"{result.source_dates:,}")
            self.card_source_rows.set_value(f"{result.source_rows:,}")
            self.card_status.set_value(result.status.value)
            self.card_rows_written.set_value(f"{result.rows_written:,}")
            self.card_file_size.set_value(
                f"{result.file_size:,} bytes" if result.file_size is not None else "—"
            )
            self.card_last_build.set_value(result.last_build or "—")
            self.card_legacy.set_value(f"{result.legacy_partition_count:,}")
            self.card_action.set_value(result.action.value)
            self.card_duration.set_value(f"{result.duration_ms / 1000.0:.2f} s")
            self.txt_output_path.setText(str(result.output_path))
            self.txt_source_identity.setText(result.source_identity or "")
            mode = "Dry Run" if result.dry_run else "Apply"
            self.lbl_status.setText(
                f"Consolidated export {mode} finished: {result.status.value}; "
                f"{result.source_dates:,} source dates, {result.source_rows:,} rows."
            )
            if result.errors:
                self._show_error("; ".join(result.errors))
            if not result.dry_run and result.synchronized and self.on_export_success:
                try:
                    self.on_export_success()
                except Exception:
                    logger.exception("failed to trigger on_export_success callback")
        except Exception as exc:
            logger.exception("error rendering consolidated Parquet results")
            self._show_error(f"Error rendering Parquet results: {exc}")
            self.lbl_status.setText("Parquet export results rendering failed.")

    def _on_export_error(self, error_msg: str) -> None:
        self._show_error(f"Parquet export failed: {error_msg}")
        self.lbl_status.setText("Parquet export failed due to error.")

    def _on_worker_finished(self) -> None:
        self.progress_bar.setVisible(False)
        self._set_controls_enabled(True)
        self.active_worker = None
