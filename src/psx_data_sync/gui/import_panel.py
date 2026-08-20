"""Historical CSV import panel widget for PSX Data Sync desktop GUI."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QFileDialog,
    QFormLayout,
    QFrame,
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

from ..importer import BatchImportResult, LocalFileImportResult, import_local_csv_directory
from .dashboard import MetricCard
from .workers import BaseWorker

if TYPE_CHECKING:
    from ..state_db import StateRepository

logger = logging.getLogger(__name__)


class ImportWidget(QWidget):
    """GUI Panel for running local historical CSV dry-run planning and imports."""

    def __init__(
        self,
        repository: StateRepository,
        on_import_success: Callable[[], None] | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.repository = repository
        self.on_import_success = on_import_success
        self.last_result: BatchImportResult | None = None
        self.active_worker: BaseWorker | None = None

        self._init_ui()

    def _init_ui(self) -> None:
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(16, 16, 16, 16)
        main_layout.setSpacing(12)

        # Header Title & Path Selector Box
        controls_group = QGroupBox("Import Configuration & Source Location")
        controls_layout = QVBoxLayout(controls_group)
        controls_layout.setSpacing(10)

        path_layout = QHBoxLayout()
        lbl_path = QLabel("Source Directory:")
        lbl_path.setStyleSheet("font-weight: bold;")
        path_layout.addWidget(lbl_path)

        self.txt_source = QLineEdit()
        self.txt_source.setPlaceholderText("Select directory containing historical CSV files...")
        path_layout.addWidget(self.txt_source)

        self.btn_browse = QPushButton("Browse...")
        self.btn_browse.clicked.connect(self._browse_directory)
        path_layout.addWidget(self.btn_browse)

        controls_layout.addLayout(path_layout)

        options_layout = QHBoxLayout()
        self.chk_recursive = QCheckBox("Include Subdirectories (Recursive)")
        self.chk_recursive.setChecked(False)
        options_layout.addWidget(self.chk_recursive)

        options_layout.addStretch()

        self.btn_dry_run = QPushButton("Dry Run (Plan Only)")
        self.btn_dry_run.setStyleSheet("font-weight: bold;")
        self.btn_dry_run.clicked.connect(lambda: self.run_import(dry_run=True))
        options_layout.addWidget(self.btn_dry_run)

        self.btn_apply = QPushButton("Apply Import")
        self.btn_apply.setStyleSheet("font-weight: bold; background-color: #0066cc; color: white;")
        self.btn_apply.clicked.connect(lambda: self.run_import(dry_run=False))
        options_layout.addWidget(self.btn_apply)

        controls_layout.addLayout(options_layout)

        main_layout.addWidget(controls_group)

        # Status Indicator & Error Display Bar
        status_layout = QHBoxLayout()
        self.lbl_status = QLabel("Ready. Select a source directory to begin.")
        self.lbl_status.setStyleSheet("font-size: 13px; color: #444;")
        status_layout.addWidget(self.lbl_status)

        status_layout.addStretch()

        self.error_label = QLabel("")
        self.error_label.setStyleSheet("color: red; font-weight: bold;")
        self.error_label.setVisible(False)
        status_layout.addWidget(self.error_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)  # Indeterminate
        self.progress_bar.setFixedWidth(150)
        self.progress_bar.setVisible(False)
        status_layout.addWidget(self.progress_bar)

        main_layout.addLayout(status_layout)

        # Summary Metrics Panel
        summary_group = QGroupBox("Import Execution Summary")
        summary_grid = QGridLayout(summary_group)
        summary_grid.setSpacing(10)

        self.card_discovered = MetricCard("Discovered", "—")
        self.card_candidates = MetricCard("Candidates", "—")
        self.card_importable = MetricCard("Importable", "—")
        self.card_imported = MetricCard("Imported", "—")
        self.card_already_present = MetricCard("Already Present", "—")
        self.card_invalid = MetricCard("Invalid", "—")
        self.card_conflicts = MetricCard("Conflicts", "—")
        self.card_unsupported = MetricCard("Unsupported", "—")
        self.card_failed = MetricCard("Failed", "—")
        self.card_rejected_rows = MetricCard("Rejected Rows", "—")
        self.card_duration = MetricCard("Duration", "—")

        summary_grid.addWidget(self.card_discovered, 0, 0)
        summary_grid.addWidget(self.card_candidates, 0, 1)
        summary_grid.addWidget(self.card_importable, 0, 2)
        summary_grid.addWidget(self.card_imported, 0, 3)
        summary_grid.addWidget(self.card_already_present, 0, 4)

        summary_grid.addWidget(self.card_invalid, 1, 0)
        summary_grid.addWidget(self.card_conflicts, 1, 1)
        summary_grid.addWidget(self.card_unsupported, 1, 2)
        summary_grid.addWidget(self.card_failed, 1, 3)
        summary_grid.addWidget(self.card_rejected_rows, 1, 4)

        main_layout.addWidget(summary_group)

        # Per-File Detailed Result Table
        table_group = QGroupBox("Per-File Inspection & Execution Details")
        table_layout = QVBoxLayout(table_group)
        table_layout.setContentsMargins(8, 8, 8, 8)

        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels([
            "Market Date",
            "Filename",
            "Action",
            "Valid Rows",
            "Rejected Rows",
            "Details / Warnings / Error",
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

    def _browse_directory(self) -> None:
        selected_dir = QFileDialog.getExistingDirectory(
            self,
            "Select Source Directory for Historical CSVs",
            self.txt_source.text() or str(Path.home()),
        )
        if selected_dir:
            self.txt_source.setText(selected_dir)

    def _set_controls_enabled(self, enabled: bool) -> None:
        self.txt_source.setEnabled(enabled)
        self.btn_browse.setEnabled(enabled)
        self.chk_recursive.setEnabled(enabled)
        self.btn_dry_run.setEnabled(enabled)
        self.btn_apply.setEnabled(enabled)

    def run_import(self, dry_run: bool = True) -> None:
        """Execute dry-run or apply mode historical import via background worker."""

        if self.active_worker is not None and self.active_worker.isRunning():
            return

        source_str = self.txt_source.text().strip()
        if not source_str:
            self._show_error("Please select a source directory.")
            return

        source_path = Path(source_str)
        if not source_path.exists() or not source_path.is_dir():
            self._show_error(f"Source directory does not exist: {source_path}")
            return

        if not dry_run:
            confirm = QMessageBox.question(
                self,
                "Confirm Historical Import",
                f"Are you sure you want to import historical CSV files from:\n{source_path}\n\n"
                "This will write canonical CSV files to the data store and index them in the database.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if confirm != QMessageBox.StandardButton.Yes:
                self.lbl_status.setText("Import cancelled by user.")
                return

        self.error_label.setVisible(False)
        self.lbl_status.setText(
            f"Running {'Dry Run' if dry_run else 'Apply'} historical import on {source_path.name}..."
        )
        self.progress_bar.setVisible(True)
        self._set_controls_enabled(False)

        raw_dir = self.repository.project_root / "data" / "raw"
        recursive = self.chk_recursive.isChecked()

        worker = BaseWorker(
            import_local_csv_directory,
            self.repository,
            source_path,
            destination_dir=raw_dir,
            dry_run=dry_run,
            recursive=recursive,
        )
        worker.signals.result.connect(self._on_import_completed)
        worker.signals.error.connect(self._on_import_error)
        worker.signals.finished.connect(self._on_worker_finished)
        worker.finished.connect(worker.deleteLater)

        self.active_worker = worker
        worker.start()

    def _on_import_completed(self, result: BatchImportResult) -> None:
        try:
            self.last_result = result
            mode_str = "Dry Run" if result.dry_run else "Apply"
            total_rejected = sum(
                r.rejected_row_count
                for r in result.results
                if r.rejected_row_count is not None
            )

            # Update Summary Metric Cards
            self.card_discovered.set_value(f"{result.discovered_count:,}")
            self.card_candidates.set_value(f"{result.candidate_count:,}")
            self.card_importable.set_value(f"{result.importable_count:,}")
            self.card_imported.set_value(f"{result.imported_count:,}")
            self.card_already_present.set_value(f"{result.already_present_count:,}")
            self.card_invalid.set_value(f"{result.invalid_count:,}")
            self.card_conflicts.set_value(f"{result.conflict_count:,}")
            self.card_unsupported.set_value(f"{result.unsupported_count:,}")
            self.card_failed.set_value(f"{result.failed_count:,}")
            self.card_rejected_rows.set_value(f"{total_rejected:,}")
            self.card_duration.set_value(f"{result.duration_ms / 1000.0:.2f} s")

            # Populate Per-File Table
            self.table.setSortingEnabled(False)
            self.table.setRowCount(len(result.results))

            for row_idx, r in enumerate(result.results):
                action_str = (
                    r.action.value if hasattr(r.action, "value") else str(r.action)
                )
                row_cnt_str = (
                    f"{r.row_count:,}" if r.row_count is not None else "0"
                )
                rej_cnt_str = (
                    f"{r.rejected_row_count:,}"
                    if r.rejected_row_count is not None
                    else "0"
                )

                item_date = QTableWidgetItem(r.market_date or "—")
                item_file = QTableWidgetItem(r.source_path.name)
                item_action = QTableWidgetItem(action_str)
                item_valid = QTableWidgetItem(row_cnt_str)
                item_rejected = QTableWidgetItem(rej_cnt_str)

                details_parts: list[str] = []
                if r.error:
                    details_parts.append(f"Error: {r.error}")
                if r.warnings:
                    details_parts.append(f"Warnings: {'; '.join(r.warnings)}")
                if not details_parts:
                    details_parts.append("OK")
                item_details = QTableWidgetItem(" | ".join(details_parts))

                # Alignment
                item_valid.setTextAlignment(
                    Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
                )
                item_rejected.setTextAlignment(
                    Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
                )

                self.table.setItem(row_idx, 0, item_date)
                self.table.setItem(row_idx, 1, item_file)
                self.table.setItem(row_idx, 2, item_action)
                self.table.setItem(row_idx, 3, item_valid)
                self.table.setItem(row_idx, 4, item_rejected)
                self.table.setItem(row_idx, 5, item_details)

            self.table.setSortingEnabled(True)

            self.lbl_status.setText(
                f"{mode_str} finished in {result.duration_ms / 1000.0:.2f} s. "
                f"Discovered: {result.discovered_count}, Imported: {result.imported_count}, "
                f"Already Present: {result.already_present_count}, Invalid/Conflicts: {result.invalid_count + result.conflict_count}."
            )

            if not result.dry_run and self.on_import_success:
                try:
                    self.on_import_success()
                except Exception as exc:
                    logger.exception("failed to trigger on_import_success callback")
        except Exception as exc:
            logger.exception("error rendering import results")
            self._show_error(f"Error rendering import results: {exc}")
            self.lbl_status.setText("Import results rendering failed.")

    def _on_import_error(self, error_msg: str) -> None:
        self._show_error(f"Import process failed: {error_msg}")
        self.lbl_status.setText("Import failed due to error.")

    def _on_worker_finished(self) -> None:
        self.progress_bar.setVisible(False)
        self._set_controls_enabled(True)
        self.active_worker = None

    def _show_error(self, message: str) -> None:
        self.error_label.setText(message)
        self.error_label.setVisible(True)
