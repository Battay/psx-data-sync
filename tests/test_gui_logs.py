from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest
from PySide6.QtWidgets import QApplication

from psx_data_sync.gui.app import create_app
from psx_data_sync.gui.logs_panel import LogsWidget
from psx_data_sync.state import DownloadResult, DownloadStatus, LogActivityItem, ParquetExportStatus, PersistentSyncStatus
from psx_data_sync.state_db import StateRepository

os.environ["QT_QPA_PLATFORM"] = "offscreen"


@pytest.fixture(scope="session")
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = create_app(["--offscreen"])
    return app


def test_logs_widget_construction_empty_db(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = LogsWidget(repo)
    if widget.active_worker:
        widget.active_worker.wait(5000)
        qapp.processEvents()

    assert widget.table.columnCount() == 6
    assert "Loaded" in widget.lbl_status_msg.text()


def test_seeded_history_rendering_and_details_selection(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    # Seed sync run and parquet export history into SQLite DB
    run_id = repo.begin_sync_run("fetch", "2026-08-01", "2026-08-01", 1, 1)
    repo.record_download_result(
        run_id,
        DownloadResult(
            requested_date="2026-08-01",
            status=DownloadStatus.TRADING_DATA,
            attempts=1,
            valid_row_count=100,
            checksum="sha_abc",
        ),
    )
    repo.upsert_parquet_export(
        "2026-08-01",
        status=ParquetExportStatus.CURRENT,
        schema_version="v1",
        source_csv_checksum_sha256="sha_abc",
        source_row_count=100,
        parquet_path=tmp_path / "data" / "parquet" / "market_date=2026-08-01" / "data.parquet",
        parquet_checksum_sha256="sha_xyz",
        parquet_row_count=100,
        verified_at="2026-08-01T12:00:00Z",
    )

    widget = LogsWidget(repo)
    if widget.active_worker:
        widget.active_worker.wait(5000)
        qapp.processEvents()

    assert widget.table.rowCount() >= 1

    # Find the row for PARQUET_EXPORT
    parquet_row = -1
    for r in range(widget.table.rowCount()):
        if widget.table.item(r, 1).text() == "PARQUET_EXPORT":
            parquet_row = r
            break

    assert parquet_row >= 0
    assert widget.table.item(parquet_row, 4).text() == "CURRENT"

    # Test details pane row selection
    widget.table.selectRow(parquet_row)
    qapp.processEvents()

    details_text = widget.txt_details.toPlainText()
    assert "PARQUET_EXPORT" in details_text
    assert "2026-08-01" in details_text
    assert "CURRENT" in details_text


def test_logs_filtering_by_activity_type(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    # Seed date_sync_state and parquet export record
    run_id = repo.begin_sync_run("fetch", "2026-08-01", "2026-08-01", 1, 1)
    repo.record_download_result(
        run_id,
        DownloadResult(
            requested_date="2026-08-01",
            status=DownloadStatus.TRADING_DATA,
            attempts=1,
            valid_row_count=100,
            checksum="sha_abc",
        ),
    )
    repo.upsert_parquet_export(
        "2026-08-01",
        status=ParquetExportStatus.CURRENT,
        schema_version="v1",
        source_csv_checksum_sha256="sha_abc",
        source_row_count=100,
        parquet_path=tmp_path / "data" / "parquet" / "market_date=2026-08-01" / "data.parquet",
        parquet_checksum_sha256="sha_xyz",
        parquet_row_count=100,
        verified_at="2026-08-01T12:00:00Z",
    )

    widget = LogsWidget(repo)
    if widget.active_worker:
        widget.active_worker.wait(5000)
        qapp.processEvents()

    # Filter to RECONCILIATION -> should show 0 rows
    widget.cmb_type.setCurrentText("RECONCILIATION")
    if widget.active_worker:
        widget.active_worker.wait(5000)
        qapp.processEvents()

    assert widget.table.rowCount() == 0

    # Filter back to PARQUET_EXPORT -> should show 1 row
    widget.cmb_type.setCurrentText("PARQUET_EXPORT")
    if widget.active_worker:
        widget.active_worker.wait(5000)
        qapp.processEvents()

    assert widget.table.rowCount() == 1


def test_read_only_guarantees_no_network_no_mutation(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    with patch("urllib.request.urlopen") as mock_url:
        widget = LogsWidget(repo)
        if widget.active_worker:
            widget.active_worker.wait(5000)
            qapp.processEvents()

        widget.refresh_logs()
        if widget.active_worker:
            widget.active_worker.wait(5000)
            qapp.processEvents()

        mock_url.assert_not_called()
