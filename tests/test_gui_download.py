from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from PySide6.QtWidgets import QApplication, QMessageBox

from psx_data_sync.config import DEFAULT_RANGE_WORKERS
from psx_data_sync.gui.app import create_app
from psx_data_sync.gui.download_panel import DownloadWidget
from psx_data_sync.state import DownloadResult, DownloadStatus, RangeDownloadResult
from psx_data_sync.state_db import StateRepository

os.environ["QT_QPA_PLATFORM"] = "offscreen"


@pytest.fixture(scope="session")
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = create_app(["--offscreen"])
    return app


def _dummy_range_result(
    start_date: str = "2026-08-01",
    end_date: str = "2026-08-02",
) -> RangeDownloadResult:
    res1 = DownloadResult(
        requested_date="2026-08-01",
        status=DownloadStatus.TRADING_DATA,
        attempts=1,
        valid_row_count=150,
        http_status=200,
        checksum="abc1",
    )
    res2 = DownloadResult(
        requested_date="2026-08-02",
        status=DownloadStatus.TRADING_DATA,
        attempts=0,
        valid_row_count=150,
        checksum="abc2",
        locally_skipped=True,
    )
    return RangeDownloadResult(
        start_date=start_date,
        end_date=end_date,
        requested_dates=(start_date, end_date),
        workers=4,
        total_duration_ms=1250.0,
        results=(res1, res2),
        counts_by_status={DownloadStatus.TRADING_DATA: 2},
        total_parsed_rows=300,
        total_valid_rows=300,
        total_rejected_rows=0,
        total_response_bytes=1000,
        total_retries=0,
        rate_limit_occurrences=0,
        network_fetched_dates=1,
        locally_skipped_dates=1,
        verified_successful_dates=2,
        failed_dates=(),
        unresolved_empty_dates=(),
        average_per_date_duration_ms=625.0,
        dates_per_second=1.6,
        verified_dates_per_second=1.6,
        network_dates_per_second=0.8,
        rows_per_second=240.0,
    )


def test_download_widget_construction_no_network(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    with patch("psx_data_sync.cli.run_range_download") as mock_backend:
        widget = DownloadWidget(repo)
        assert widget.txt_start_date is not None
        assert widget.txt_end_date is not None
        assert widget.txt_start_date.calendarPopup() is True
        assert widget.txt_start_date.displayFormat() == "yyyy-MM-dd"
        assert widget.spin_workers.value() == DEFAULT_RANGE_WORKERS
        assert widget.table.columnCount() == 6
        mock_backend.assert_not_called()


def test_date_validation_errors(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = DownloadWidget(repo)

    # Start date after end date
    widget.txt_start_date.set_date_val("2026-08-10")
    widget.txt_end_date.set_date_val("2026-08-05")
    widget.run_download()
    assert "Start date cannot be after end date" in widget.error_label.text()


def test_large_range_confirmation_cancellation_aborts_download(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = DownloadWidget(repo)
    # Range > 90 days (100 days)
    widget.txt_start_date.set_date_val("2026-01-01")
    widget.txt_end_date.set_date_val("2026-04-10")

    with patch("psx_data_sync.gui.download_panel.run_range_download") as mock_backend:
        with patch.object(QMessageBox, "question", return_value=QMessageBox.StandardButton.No):
            widget.run_download()

        assert widget.lbl_status.text() == "Range download cancelled by user."
        mock_backend.assert_not_called()


def test_successful_range_download_and_control_locking(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    callback_mock = MagicMock()
    widget = DownloadWidget(repo, on_download_success=callback_mock)
    widget.txt_start_date.set_date_val("2026-08-01")
    widget.txt_end_date.set_date_val("2026-08-02")
    widget.spin_workers.setValue(4)

    dummy_res = _dummy_range_result()

    with patch("psx_data_sync.gui.download_panel.run_range_download", return_value=dummy_res) as mock_backend:
        widget.run_download()

        # Controls disabled during execution
        assert widget.btn_download.isEnabled() is False
        assert widget.txt_start_date.isEnabled() is False

        if widget.active_worker:
            widget.active_worker.wait(5000)
            qapp.processEvents()

        mock_backend.assert_called_once()

    # Controls restored after completion
    assert widget.btn_download.isEnabled() is True
    assert widget.txt_start_date.isEnabled() is True
    assert widget.progress_bar.isVisible() is False
    assert widget.active_worker is None

    # Verify summary cards and per-date table
    assert widget.card_requested.value_label.text() == "2"
    assert widget.card_verified.value_label.text() == "2"
    assert widget.card_skipped.value_label.text() == "1"
    assert widget.card_total_rows.value_label.text() == "300"
    assert widget.table.rowCount() == 2
    assert "LOCAL_SKIP" in widget.table.item(1, 1).text()

    # Verify dashboard refresh callback was triggered
    callback_mock.assert_called_once()


def test_download_error_handling_restores_controls(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = DownloadWidget(repo)
    widget.txt_start_date.set_date_val("2026-08-01")
    widget.txt_end_date.set_date_val("2026-08-02")

    with patch("psx_data_sync.gui.download_panel.run_range_download", side_effect=RuntimeError("Synthetic network failure")):
        widget.run_download()

        if widget.active_worker:
            widget.active_worker.wait(5000)
            qapp.processEvents()

    assert widget.btn_download.isEnabled() is True
    assert widget.progress_bar.isVisible() is False
    assert widget.active_worker is None
    assert widget.lbl_status.text() == "Download failed due to error."
    assert "Synthetic network failure" in widget.error_label.text()
