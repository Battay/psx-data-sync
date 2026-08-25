from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from PySide6.QtWidgets import QApplication, QMessageBox

from psx_data_sync.gui.app import create_app
from psx_data_sync.gui.parquet_panel import ParquetExportWidget
from psx_data_sync.parquet_sync import (
    ConsolidatedParquetSyncResult,
    ParquetExportAction,
)
from psx_data_sync.state import ParquetExportStatus
from psx_data_sync.state_db import StateRepository

os.environ["QT_QPA_PLATFORM"] = "offscreen"


@pytest.fixture(scope="session")
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = create_app(["--offscreen"])
    return app


def _dummy_result(tmp_path: Path, *, dry_run: bool) -> ConsolidatedParquetSyncResult:
    return ConsolidatedParquetSyncResult(
        source_dates=4,
        source_rows=2371,
        status=ParquetExportStatus.MISSING if dry_run else ParquetExportStatus.CURRENT,
        planned_status=ParquetExportStatus.CURRENT,
        action=ParquetExportAction.CREATE,
        rows_written=0 if dry_run else 2371,
        output_path=tmp_path / "data" / "parquet" / "market.parquet",
        source_identity="a" * 64,
        legacy_partition_count=3,
        file_size=None if dry_run else 45678,
        last_build=None if dry_run else "2026-08-25T10:00:00+00:00",
        dry_run=dry_run,
        rebuild=False,
        duration_ms=450.0,
    )


def test_widget_is_consolidated_and_has_no_range_or_partition_table(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()
    with patch("psx_data_sync.gui.parquet_panel.sync_consolidated_parquet") as backend:
        widget = ParquetExportWidget(repo)
        assert not hasattr(widget, "txt_start_date")
        assert not hasattr(widget, "txt_end_date")
        assert not hasattr(widget, "table")
        assert widget.card_source_dates is not None
        assert widget.card_source_rows is not None
        assert widget.card_status is not None
        assert widget.txt_output_path.isReadOnly()
        backend.assert_not_called()


def test_rebuild_rejected_during_dry_run(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()
    widget = ParquetExportWidget(repo)
    widget.chk_rebuild.setChecked(True)
    with patch("psx_data_sync.gui.parquet_panel.sync_consolidated_parquet") as backend:
        widget.run_export(dry_run=True)
        assert "Rebuild can only be used with Apply mode" in widget.error_label.text()
        backend.assert_not_called()


def test_dry_run_service_compatibility(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()
    widget = ParquetExportWidget(repo)
    dummy = _dummy_result(tmp_path, dry_run=True)
    with patch(
        "psx_data_sync.gui.parquet_panel.sync_consolidated_parquet",
        return_value=dummy,
    ) as backend:
        widget.run_export(dry_run=True)
        if widget.active_worker:
            widget.active_worker.wait(5000)
            qapp.processEvents()
        backend.assert_called_once_with(
            repo,
            output_root=repo.raw_output_dir.parent / "parquet",
            dry_run=True,
            rebuild=False,
        )
    assert widget.last_result == dummy
    assert widget.card_source_dates.value_label.text() == "4"
    assert widget.card_source_rows.value_label.text() == "2,371"
    assert widget.card_status.value_label.text() == "MISSING"
    assert widget.card_legacy.value_label.text() == "3"
    assert widget.txt_output_path.text().endswith("data/parquet/market.parquet")


def test_apply_confirmation_and_success_callback(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()
    callback = MagicMock()
    widget = ParquetExportWidget(repo, on_export_success=callback)
    dummy = _dummy_result(tmp_path, dry_run=False)
    with patch(
        "psx_data_sync.gui.parquet_panel.sync_consolidated_parquet",
        return_value=dummy,
    ) as backend, patch.object(
        QMessageBox,
        "question",
        return_value=QMessageBox.StandardButton.Yes,
    ):
        widget.run_export(dry_run=False)
        if widget.active_worker:
            widget.active_worker.wait(5000)
            qapp.processEvents()
        backend.assert_called_once()
    callback.assert_called_once()
    assert widget.card_status.value_label.text() == "CURRENT"
    assert widget.card_rows_written.value_label.text() == "2,371"
    assert widget.btn_apply.isEnabled()


def test_apply_cancellation_does_not_start_service(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()
    widget = ParquetExportWidget(repo)
    with patch("psx_data_sync.gui.parquet_panel.sync_consolidated_parquet") as backend, patch.object(
        QMessageBox,
        "question",
        return_value=QMessageBox.StandardButton.No,
    ):
        widget.run_export(dry_run=False)
    backend.assert_not_called()
    assert widget.lbl_status.text() == "Parquet export cancelled by user."
