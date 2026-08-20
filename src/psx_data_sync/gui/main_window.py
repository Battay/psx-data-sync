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
from .import_panel import ImportWidget

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
        self.resize(1024, 700)

        self._init_ui()

    def _init_ui(self) -> None:
        central = QWidget(self)
        self.setCentralWidget(central)

        main_layout = QHBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # Sidebar navigation panel
        sidebar_container = QWidget()
        sidebar_container.setFixedWidth(200)
        sidebar_container.setStyleSheet(
            "background-color: #f4f5f7; border-right: 1px solid #e1e4e8;"
        )
        sidebar_layout = QVBoxLayout(sidebar_container)
        sidebar_layout.setContentsMargins(8, 16, 8, 16)

        app_title = QLabel("PSX Data Sync")
        app_title.setStyleSheet(
            "font-size: 16px; font-weight: bold; padding-left: 8px; margin-bottom:"
            " 12px;"
        )
        sidebar_layout.addWidget(app_title)

        self.nav_list = QListWidget()
        self.nav_list.setStyleSheet(
            "QListWidget { border: none; background: transparent; font-size:"
            " 14px; }\nQListWidget::item { padding: 10px; border-radius: 4px;"
            " }\nQListWidget::item:selected { background-color: #0066cc; color:"
            " white; }"
        )

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

        # Pages 2-5: Placeholders
        for title in self.NAV_ITEMS[2:]:
            placeholder = PlaceholderWidget(title)
            self.stack.addWidget(placeholder)

        main_layout.addWidget(self.stack)

        # Connect navigation selection
        self.nav_list.currentRowChanged.connect(self.stack.setCurrentIndex)
        self.nav_list.setCurrentRow(0)
