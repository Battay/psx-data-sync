"""Read-only logs and operational activity viewer widget for PSX Data Sync desktop GUI."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from ..state import LogActivityItem
from .workers import BaseWorker

if TYPE_CHECKING:
    from ..state_db import StateRepository

logger = logging.getLogger(__name__)


class LogsWidget(QWidget):
    """Read-only operational logs and execution history viewer."""

    ACTIVITY_TYPES: tuple[str, ...] = (
        "ALL",
        "SYNC_RUN",
        "RECONCILIATION",
        "DOWNLOAD_ATTEMPT",
        "PARQUET_EXPORT",
    )

    def __init__(
        self,
        repository: StateRepository,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.repository = repository
        self.log_items: tuple[LogActivityItem, ...] = ()
        self.active_worker: BaseWorker | None = None

        self._init_ui()
        self.refresh_logs()

    def _init_ui(self) -> None:
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(16, 16, 16, 16)
        main_layout.setSpacing(12)

        # Controls Box
        controls_group = QGroupBox("Activity Logs Filter & Controls")
        controls_layout = QHBoxLayout(controls_group)
        controls_layout.setSpacing(12)

        lbl_type = QLabel("Activity Type:")
        lbl_type.setStyleSheet("font-weight: bold;")
        controls_layout.addWidget(lbl_type)

        self.cmb_type = QComboBox()
        self.cmb_type.addItems(self.ACTIVITY_TYPES)
        self.cmb_type.setFixedWidth(160)
        self.cmb_type.currentTextChanged.connect(self.refresh_logs)
        controls_layout.addWidget(self.cmb_type)

        lbl_status = QLabel("Status Filter:")
        lbl_status.setStyleSheet("font-weight: bold;")
        controls_layout.addWidget(lbl_status)

        self.txt_status_filter = QLineEdit()
        self.txt_status_filter.setPlaceholderText("Filter status (e.g. VERIFIED, FAILED)...")
        self.txt_status_filter.setFixedWidth(200)
        self.txt_status_filter.textChanged.connect(self.refresh_logs)
        controls_layout.addWidget(self.txt_status_filter)

        lbl_limit = QLabel("Limit:")
        lbl_limit.setStyleSheet("font-weight: bold;")
        controls_layout.addWidget(lbl_limit)

        self.cmb_limit = QComboBox()
        self.cmb_limit.addItems(["200", "500", "1000"])
        self.cmb_limit.setFixedWidth(80)
        self.cmb_limit.currentTextChanged.connect(self.refresh_logs)
        controls_layout.addWidget(self.cmb_limit)

        controls_layout.addStretch()

        self.btn_refresh = QPushButton("Refresh Logs")
        self.btn_refresh.setProperty("accent", True)
        self.btn_refresh.clicked.connect(self.refresh_logs)
        controls_layout.addWidget(self.btn_refresh)

        main_layout.addWidget(controls_group)

        # Status Bar
        self.lbl_status_msg = QLabel("Ready. Loading activity logs...")
        self.lbl_status_msg.setStyleSheet("font-size: 13px; color: #94a3b8;")
        main_layout.addWidget(self.lbl_status_msg)

        # Main Splitter (Table + Details Text View)
        splitter = QSplitter(Qt.Orientation.Vertical)

        # Upper Container: Table
        table_container = QWidget()
        table_layout = QVBoxLayout(table_container)
        table_layout.setContentsMargins(0, 0, 0, 0)

        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        v_header = self.table.verticalHeader()
        if v_header is not None:
            v_header.setDefaultSectionSize(30)
            v_header.setVisible(False)

        self.table.setHorizontalHeaderLabels([
            "Timestamp",
            "Activity Type",
            "Reference / Run ID",
            "Market Date / Range",
            "Status",
            "Metrics / Summary",
        ])
        header = self.table.horizontalHeader()
        if header is not None:
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)

        self.table.itemSelectionChanged.connect(self._on_table_row_selected)
        table_layout.addWidget(self.table)
        splitter.addWidget(table_container)

        # Lower Container: Details Pane
        details_group = QGroupBox("Selected Activity Item Details")
        details_layout = QVBoxLayout(details_group)
        details_layout.setContentsMargins(8, 8, 8, 8)

        self.txt_details = QTextEdit()
        self.txt_details.setReadOnly(True)
        self.txt_details.setPlaceholderText("Select an entry in the table above to view complete execution details...")
        self.txt_details.setStyleSheet(
            "background-color: #0f172a; color: #f8fafc; font-family: monospace; font-size: 12px;"
        )
        details_layout.addWidget(self.txt_details)
        splitter.addWidget(details_group)

        splitter.setSizes([420, 180])
        main_layout.addWidget(splitter)

    def refresh_logs(self) -> None:
        """Fetch read-only log records asynchronously without state mutations or network requests."""

        if self.active_worker is not None and self.active_worker.isRunning():
            return

        activity_type = self.cmb_type.currentText()
        if activity_type == "ALL":
            activity_type = None

        status_filter = self.txt_status_filter.text().strip() or None
        limit = int(self.cmb_limit.currentText())

        self.lbl_status_msg.setText("Refreshing activity logs...")
        self.btn_refresh.setEnabled(False)

        worker = BaseWorker(
            self.repository.get_activity_logs,
            limit=limit,
            activity_type_filter=activity_type,
            status_filter=status_filter,
        )
        worker.signals.result.connect(self._on_logs_loaded)
        worker.signals.error.connect(self._on_logs_error)
        worker.signals.finished.connect(self._on_worker_finished)
        worker.finished.connect(worker.deleteLater)

        self.active_worker = worker
        worker.start()

    def _on_logs_loaded(self, items: tuple[LogActivityItem, ...]) -> None:
        self.log_items = items
        self.table.setSortingEnabled(False)
        self.table.setRowCount(len(items))

        for row_idx, item in enumerate(items):
            item_ts = QTableWidgetItem(item.timestamp)
            item_type = QTableWidgetItem(item.activity_type)
            item_ref = QTableWidgetItem(item.reference_id[:16] if len(item.reference_id) > 16 else item.reference_id)
            item_date = QTableWidgetItem(item.market_date_or_range)
            item_status = QTableWidgetItem(item.status)
            item_summary = QTableWidgetItem(item.metrics_summary)

            item_status.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)

            self.table.setItem(row_idx, 0, item_ts)
            self.table.setItem(row_idx, 1, item_type)
            self.table.setItem(row_idx, 2, item_ref)
            self.table.setItem(row_idx, 3, item_date)
            self.table.setItem(row_idx, 4, item_status)
            self.table.setItem(row_idx, 5, item_summary)

        self.table.setSortingEnabled(True)
        self.lbl_status_msg.setText(f"Loaded {len(items):,} activity log entries.")
        self.txt_details.clear()

    def _on_logs_error(self, error_msg: str) -> None:
        self.lbl_status_msg.setText(f"Error loading logs: {error_msg}")

    def _on_worker_finished(self) -> None:
        self.btn_refresh.setEnabled(True)
        self.active_worker = None

    def _on_table_row_selected(self) -> None:
        selected_rows = self.table.selectionModel().selectedRows()
        if not selected_rows:
            self.txt_details.clear()
            return

        row_idx = selected_rows[0].row()
        if 0 <= row_idx < len(self.log_items):
            item = self.log_items[row_idx]
            formatted_details = (
                f"Timestamp:            {item.timestamp}\n"
                f"Activity Type:        {item.activity_type}\n"
                f"Reference / Run ID:   {item.reference_id}\n"
                f"Market Date / Range:  {item.market_date_or_range}\n"
                f"Status:               {item.status}\n"
                f"Metrics / Summary:    {item.metrics_summary}\n"
                f"--------------------------------------------------\n"
                f"Detailed Execution Info:\n{item.details}"
            )
            self.txt_details.setText(formatted_details)
