"""Main application window for PSX Data Sync desktop GUI."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)

from .dashboard import DashboardWidget
from .download_panel import DownloadWidget
from .import_panel import ImportWidget
from .logs_panel import LogsWidget
from .parquet_panel import ParquetExportWidget
from .reconciliation_panel import ReconciliationWidget

if TYPE_CHECKING:
    from ..state_db import StateRepository

logger = logging.getLogger(__name__)


class PlaceholderWidget(QWidget):
    """Placeholder view for future D6.3+ features."""

    def __init__(self, title: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        heading = QLabel(f"{title} Workflow")
        heading.setStyleSheet("font-size: 18px; font-weight: bold; color: #666;")
        layout.addWidget(heading)

        sub = QLabel("This module will be enabled in subsequent releases.")
        sub.setStyleSheet("font-size: 13px; color: #888;")
        layout.addWidget(sub)


class PSXMainWindow(QMainWindow):
    """Primary window container for PSX Data Sync GUI."""

    NAV_ITEMS: tuple[str, ...] = (
        "Dashboard",
        "Import",
        "Download",
        "Reconciliation",
        "Parquet Export",
        "Logs",
    )

    def __init__(
        self,
        repository: StateRepository,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.repository = repository
        self.setWindowTitle("PSX Data Sync")
        self.setMinimumSize(1100, 720)
        self.resize(1180, 760)

        self._init_ui()

    def _init_ui(self) -> None:
        central = QWidget(self)
        self.setCentralWidget(central)

        main_layout = QHBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # Sidebar navigation panel
        sidebar_container = QWidget()
        sidebar_container.setFixedWidth(220)
        sidebar_container.setStyleSheet(
            "background-color: #161e2e; border-right: 1px solid #1e293b;"
        )
        sidebar_layout = QVBoxLayout(sidebar_container)
        sidebar_layout.setContentsMargins(10, 20, 10, 20)

        app_title = QLabel("PSX Data Sync")
        app_title.setStyleSheet(
            "font-size: 17px; font-weight: 700; color: #f8fafc; padding-left: 8px; margin-bottom: 16px;"
        )
        sidebar_layout.addWidget(app_title)

        self.nav_list = QListWidget()

        for item_name in self.NAV_ITEMS:
            item = QListWidgetItem(item_name)
            self.nav_list.addItem(item)

        sidebar_layout.addWidget(self.nav_list)
        sidebar_layout.addStretch()

        main_layout.addWidget(sidebar_container)

        # Main content area stacked widget
        self.stack = QStackedWidget()

        # Page 0: Dashboard
        self.dashboard_view = DashboardWidget(self.repository)
        self.stack.addWidget(self.dashboard_view)

        # Page 1: Import Widget
        self.import_view = ImportWidget(
            self.repository,
            on_import_success=self.dashboard_view.refresh_dashboard,
        )
        self.stack.addWidget(self.import_view)

        # Page 2: Download Widget
        self.download_view = DownloadWidget(
            self.repository,
            on_download_success=self.dashboard_view.refresh_dashboard,
        )
        self.stack.addWidget(self.download_view)

        # Page 3: Reconciliation Widget
        self.reconciliation_view = ReconciliationWidget(
            self.repository,
            on_reconcile_success=self.dashboard_view.refresh_dashboard,
        )
        self.stack.addWidget(self.reconciliation_view)

        # Page 4: Parquet Export Widget
        self.parquet_view = ParquetExportWidget(
            self.repository,
            on_export_success=self.dashboard_view.refresh_dashboard,
        )
        self.stack.addWidget(self.parquet_view)

        # Page 5: Logs Widget
        self.logs_view = LogsWidget(self.repository)
        self.stack.addWidget(self.logs_view)

        main_layout.addWidget(self.stack)

        # Connect navigation selection
        self.nav_list.currentRowChanged.connect(self.stack.setCurrentIndex)
        self.nav_list.setCurrentRow(0)
