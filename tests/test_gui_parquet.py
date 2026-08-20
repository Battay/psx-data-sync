from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from PySide6.QtWidgets import QApplication, QMessageBox

from psx_data_sync.gui.app import create_app
from psx_data_sync.gui.parquet_panel import ParquetExportWidget
from psx_data_sync.parquet_sync import (
    DateParquetSyncResult,
    ParquetExportAction,
    RangeParquetSyncResult,
)
from psx_data_sync.state import ParquetExportStatus, PersistentSyncStatus
from psx_data_sync.state_db import StateRepository

os.environ["QT_QPA_PLATFORM"] = "offscreen"


@pytest.fixture(scope="session")
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = create_app(["--offscreen"])
    return app


def _dummy_parquet_result(
    start_date: str = "2026-08-01",
    end_date: str = "2026-08-02",
    dry_run: bool = True,
) -> RangeParquetSyncResult:
    res1 = DateParquetSyncResult(
        market_date="2026-08-01",
        source_status=PersistentSyncStatus.VERIFIED_TRADING_DATA,
        action=ParquetExportAction.CREATE,
        export_status_before=ParquetExportStatus.MISSING,
        export_status_planned=ParquetExportStatus.CURRENT,
        export_status_after=ParquetExportStatus.CURRENT if not dry_run else ParquetExportStatus.MISSING,
        eligible=True,
        source_csv_path=Path("data/raw/market_2026-08-01.csv"),
        source_checksum="abc1",
        source_row_count=150,
        parquet_path=Path("data/parquet/market_date=2026-08-01/data.parquet"),
        parquet_checksum="xyz1",
        parquet_row_count=150,
        dry_run=dry_run,
        rebuilt_or_written=True,
    )
    res2 = DateParquetSyncResult(
        market_date="2026-08-02",
        source_status=PersistentSyncStatus.CONFIRMED_NON_TRADING,
        action=ParquetExportAction.EXCLUDE_NON_TRADING,
        export_status_before=None,
        export_status_planned=None,
        export_status_after=None,
        eligible=False,
        source_csv_path=None,
        source_checksum=None,
        source_row_count=None,
        parquet_path=None,
        parquet_checksum=None,
        parquet_row_count=None,
        dry_run=dry_run,
        rebuilt_or_written=False,
    )
    return RangeParquetSyncResult(
        start_date=start_date,
        end_date=end_date,
        requested_count=2,
        eligible_count=1,
        current_count=0,
        create_count=1,
        stale_count=0,
        corrupt_count=0,
        reindexed_count=0,
        excluded_non_trading_count=1,
        excluded_unresolved_count=0,
        excluded_failure_count=0,
        excluded_file_issue_count=0,
        source_invalid_count=0,
        failed_count=0,
        written_or_rebuilt_count=1,
        synchronized=True,
        synchronization_percentage=100.0,
        dry_run=dry_run,
        duration_ms=450.0,
        results=(res1, res2),
    )


def test_parquet_widget_construction_no_network(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    with patch("psx_data_sync.gui.parquet_panel.sync_parquet_range") as mock_backend:
        widget = ParquetExportWidget(repo)
        assert widget.txt_start_date is not None
        assert widget.txt_end_date is not None
        assert widget.chk_rebuild.isChecked() is False
        assert widget.table.columnCount() == 7
        mock_backend.assert_not_called()


def test_rebuild_rejected_in_dry_run_mode(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = ParquetExportWidget(repo)
    widget.chk_rebuild.setChecked(True)

    with patch("psx_data_sync.gui.parquet_panel.sync_parquet_range") as mock_backend:
        widget.run_export(dry_run=True)
        assert "Rebuild can only be used with Apply mode" in widget.error_label.text()
        mock_backend.assert_not_called()


def test_date_validation_errors(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = ParquetExportWidget(repo)

    # Invalid date format
    widget.txt_start_date.setText("invalid-date")
    widget.txt_end_date.setText("2026-08-05")
    widget.run_export(dry_run=True)
    assert "Invalid date format" in widget.error_label.text()

    # Start date after end date
    widget.txt_start_date.setText("2026-08-10")
    widget.txt_end_date.setText("2026-08-05")
    widget.run_export(dry_run=True)
    assert "Start date cannot be after end date" in widget.error_label.text()


def test_large_range_confirmation_cancellation_aborts_export(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = ParquetExportWidget(repo)
    # Range > 90 days (100 days)
    widget.txt_start_date.setText("2026-01-01")
    widget.txt_end_date.setText("2026-04-10")

    with patch("psx_data_sync.gui.parquet_panel.sync_parquet_range") as mock_backend:
        with patch.object(QMessageBox, "question", return_value=QMessageBox.StandardButton.No):
            widget.run_export(dry_run=True)

        assert widget.lbl_status.text() == "Parquet export cancelled by user."
        mock_backend.assert_not_called()


def test_apply_mode_confirmation_cancellation_aborts_export(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = ParquetExportWidget(repo)
    widget.txt_start_date.setText("2026-08-01")
    widget.txt_end_date.setText("2026-08-02")

    with patch("psx_data_sync.gui.parquet_panel.sync_parquet_range") as mock_backend:
        with patch.object(QMessageBox, "question", return_value=QMessageBox.StandardButton.No):
            widget.run_export(dry_run=False)

        assert widget.lbl_status.text() == "Parquet export cancelled by user."
        mock_backend.assert_not_called()


def test_dry_run_parquet_export_execution(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = ParquetExportWidget(repo)
    widget.txt_start_date.setText("2026-08-01")
    widget.txt_end_date.setText("2026-08-02")

    dummy_res = _dummy_parquet_result(dry_run=True)

    with patch("psx_data_sync.gui.parquet_panel.sync_parquet_range", return_value=dummy_res) as mock_backend:
        widget.run_export(dry_run=True)

        if widget.active_worker:
            widget.active_worker.wait(5000)
            qapp.processEvents()

        mock_backend.assert_called_once_with(
            repo,
            "2026-08-01",
            "2026-08-02",
            dry_run=True,
            rebuild=False,
        )

    assert widget.last_result is not None
    assert widget.card_requested.value_label.text() == "2"
    assert widget.card_eligible.value_label.text() == "1"
    assert widget.card_sync_pct.value_label.text() == "100.0%"
    assert widget.table.rowCount() == 2


def test_apply_parquet_export_execution_and_callback(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    callback_mock = MagicMock()
    widget = ParquetExportWidget(repo, on_export_success=callback_mock)
    widget.txt_start_date.setText("2026-08-01")
    widget.txt_end_date.setText("2026-08-02")
    widget.chk_rebuild.setChecked(True)

    dummy_res = _dummy_parquet_result(dry_run=False)

    with patch("psx_data_sync.gui.parquet_panel.sync_parquet_range", return_value=dummy_res) as mock_backend:
        with patch.object(QMessageBox, "question", return_value=QMessageBox.StandardButton.Yes):
            widget.run_export(dry_run=False)

        if widget.active_worker:
            widget.active_worker.wait(5000)
            qapp.processEvents()

        mock_backend.assert_called_once_with(
            repo,
            "2026-08-01",
            "2026-08-02",
            dry_run=False,
            rebuild=True,
        )

    assert widget.btn_apply.isEnabled() is True
    assert widget.progress_bar.isVisible() is False
    assert widget.active_worker is None
    callback_mock.assert_called_once()
