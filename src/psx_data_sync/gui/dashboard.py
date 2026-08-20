"""Read-only dashboard widget for PSX Data Sync desktop GUI."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from PySide6.QtWidgets import (
    QFormLayout,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from ..state import DashboardSummary

if TYPE_CHECKING:
    from ..state_db import StateRepository

logger = logging.getLogger(__name__)


class MetricCard(QFrame):
    """Visual card displaying a key metric count and description."""

    def __init__(
        self,
        title: str,
        value: str = "—",
        subtitle: str = "",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setFrameShadow(QFrame.Shadow.Raised)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)

        self.title_label = QLabel(title)
        self.title_label.setStyleSheet("font-weight: bold; color: #555;")
        layout.addWidget(self.title_label)

        self.value_label = QLabel(value)
        self.value_label.setStyleSheet("font-size: 20px; font-weight: bold; color: #111;")
        layout.addWidget(self.value_label)

        if subtitle:
            self.sub_label = QLabel(subtitle)
            self.sub_label.setStyleSheet("font-size: 11px; color: #777;")
            layout.addWidget(self.sub_label)

    def set_value(self, value: str) -> None:
        self.value_label.setText(value)


class DashboardWidget(QWidget):
    """Read-only metrics dashboard for PSX Data Sync."""

    def __init__(
        self,
        repository: StateRepository,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.repository = repository
        self.summary: DashboardSummary | None = None

        self._init_ui()
        self.refresh_dashboard()

    def _init_ui(self) -> None:
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(16, 16, 16, 16)
        main_layout.setSpacing(16)

        # Header bar with title and manual Refresh button
        header_layout = QHBoxLayout()
        title = QLabel("System Overview")
        title.setStyleSheet("font-size: 18px; font-weight: bold;")
        header_layout.addWidget(title)

        header_layout.addStretch()

        self.error_label = QLabel("")
        self.error_label.setStyleSheet("color: red; font-weight: bold;")
        self.error_label.setVisible(False)
        header_layout.addWidget(self.error_label)

        self.refresh_button = QPushButton("Refresh Dashboard")
        self.refresh_button.clicked.connect(self.refresh_dashboard)
        header_layout.addWidget(self.refresh_button)

        main_layout.addLayout(header_layout)

        # Environment & System Metadata Box
        env_group = QGroupBox("System & Database Metadata")
        env_layout = QFormLayout(env_group)
        self.lbl_app_version = QLabel("—")
        self.lbl_schema_version = QLabel("—")
        self.lbl_db_path = QLabel("—")
        self.lbl_date_range = QLabel("—")

        env_layout.addRow("App Version:", self.lbl_app_version)
        env_layout.addRow("SQLite Schema Version:", self.lbl_schema_version)
        env_layout.addRow("Database Path:", self.lbl_db_path)
        env_layout.addRow("Tracked Date Range:", self.lbl_date_range)
        main_layout.addWidget(env_group)

        # Key Metric Cards Grid
        grid_layout = QGridLayout()
        grid_layout.setSpacing(12)

        self.card_total_tracked = MetricCard("Tracked Dates", "—")
        self.card_verified_trading = MetricCard("Verified Trading", "—")
        self.card_local_csv = MetricCard("Local CSV Verified", "—")
        self.card_confirmed_non_trading = MetricCard("Non-Trading", "—")
        self.card_empty_unresolved = MetricCard("Empty / Unresolved", "—")
        self.card_file_issues = MetricCard("File Issues / Corrupt", "—")
        self.card_failures = MetricCard("Failures", "—")
        self.card_canonical_csv = MetricCard("Canonical Raw CSVs", "—")

        grid_layout.addWidget(self.card_total_tracked, 0, 0)
        grid_layout.addWidget(self.card_verified_trading, 0, 1)
        grid_layout.addWidget(self.card_local_csv, 0, 2)
        grid_layout.addWidget(self.card_confirmed_non_trading, 0, 3)

        grid_layout.addWidget(self.card_empty_unresolved, 1, 0)
        grid_layout.addWidget(self.card_file_issues, 1, 1)
        grid_layout.addWidget(self.card_failures, 1, 2)
        grid_layout.addWidget(self.card_canonical_csv, 1, 3)

        main_layout.addLayout(grid_layout)

        # Parquet Export State Box
        parquet_group = QGroupBox("Parquet Partition States")
        parquet_layout = QGridLayout(parquet_group)
        parquet_layout.setSpacing(12)

        self.card_pq_current = MetricCard("Parquet CURRENT", "—")
        self.card_pq_missing = MetricCard("Parquet MISSING", "—")
        self.card_pq_stale = MetricCard("Parquet STALE", "—")
        self.card_pq_corrupt = MetricCard("Parquet CORRUPT", "—")
        self.card_pq_failed = MetricCard("Parquet FAILED", "—")

        parquet_layout.addWidget(self.card_pq_current, 0, 0)
        parquet_layout.addWidget(self.card_pq_missing, 0, 1)
        parquet_layout.addWidget(self.card_pq_stale, 0, 2)
        parquet_layout.addWidget(self.card_pq_corrupt, 0, 3)
        parquet_layout.addWidget(self.card_pq_failed, 0, 4)

        main_layout.addWidget(parquet_group)
        main_layout.addStretch()

    def refresh_dashboard(self) -> None:
        """Refresh read-only metrics from repository safely without mutating state."""

        try:
            self.error_label.setVisible(False)
            self.summary = self.repository.get_dashboard_summary()

            self.lbl_app_version.setText(self.summary.application_version)
            self.lbl_schema_version.setText(str(self.summary.schema_version))
            self.lbl_db_path.setText(str(self.summary.database_path))

            range_text = (
                f"{self.summary.earliest_date} → {self.summary.latest_date}"
                if self.summary.earliest_date and self.summary.latest_date
                else "No tracked dates"
            )
            self.lbl_date_range.setText(range_text)

            self.card_total_tracked.set_value(f"{self.summary.total_tracked_dates:,}")
            self.card_verified_trading.set_value(f"{self.summary.verified_trading_count:,}")
            self.card_local_csv.set_value(f"{self.summary.local_csv_verified_count:,}")
            self.card_confirmed_non_trading.set_value(
                f"{self.summary.confirmed_non_trading_count:,}"
            )
            self.card_empty_unresolved.set_value(
                f"{self.summary.empty_unresolved_count:,}"
            )
            self.card_file_issues.set_value(f"{self.summary.file_issue_count:,}")
            self.card_failures.set_value(f"{self.summary.failure_count:,}")
            self.card_canonical_csv.set_value(
                f"{self.summary.total_canonical_csv_count:,}"
            )

            self.card_pq_current.set_value(f"{self.summary.parquet_current_count:,}")
            self.card_pq_missing.set_value(f"{self.summary.parquet_missing_count:,}")
            self.card_pq_stale.set_value(f"{self.summary.parquet_stale_count:,}")
            self.card_pq_corrupt.set_value(f"{self.summary.parquet_corrupt_count:,}")
            self.card_pq_failed.set_value(f"{self.summary.parquet_failed_count:,}")
        except Exception as exc:
            logger.exception("failed to load dashboard summary")
            self.error_label.setText(f"Dashboard Error: {exc}")
            self.error_label.setVisible(True)
