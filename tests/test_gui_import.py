from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from PySide6.QtWidgets import QApplication, QMessageBox

from psx_data_sync.exporter import canonical_csv_bytes
from psx_data_sync.gui.app import create_app
from psx_data_sync.gui.import_panel import ImportWidget
from psx_data_sync.state import ValidEquityRow
from psx_data_sync.state_db import StateRepository

os.environ["QT_QPA_PLATFORM"] = "offscreen"


@pytest.fixture(scope="session")
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = create_app(["--offscreen"])
    return app


def _row(symbol: str = "AAA") -> ValidEquityRow:
    return ValidEquityRow(
        row_index=1,
        symbol=symbol,
        ldcp=Decimal("10.0"),
        open=Decimal("10.0"),
        high=Decimal("10.0"),
        low=Decimal("10.0"),
        close=Decimal("10.0"),
        change=Decimal("0.0"),
        change_percent=Decimal("0.0"),
        volume=100,
    )


def create_legacy_csv(path: Path, date_text: str = "2026-08-05") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = "symbol,date,ldcp,open,high,low,close,change,change_percent,volume\n"
    line = f"AAA,{date_text},10.0,10.0,10.0,10.0,10.0,0.0,0.0,100\n"
    path.write_text(header + line, encoding="utf-8")


def test_import_widget_construction(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = ImportWidget(repo)
    assert widget.txt_source is not None
    assert widget.btn_dry_run is not None
    assert widget.btn_apply is not None
    assert widget.table.columnCount() == 6


def test_invalid_source_directory_shows_error(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    widget = ImportWidget(repo)
    widget.txt_source.setText(str(tmp_path / "non_existent_folder"))

    widget.run_import(dry_run=True)
    assert "does not exist" in widget.error_label.text()


def test_dry_run_executes_worker_without_mutation(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    source_dir = tmp_path / "source_archive"
    source_file = source_dir / "market_2026-08-05.csv"
    create_legacy_csv(source_file, "2026-08-05")

    widget = ImportWidget(repo)
    widget.txt_source.setText(str(source_dir))

    widget.run_import(dry_run=True)

    # Wait for worker thread to finish execution
    if widget.active_worker:
        widget.active_worker.wait(5000)
        qapp.processEvents()

    assert widget.last_result is not None
    assert widget.last_result.dry_run is True
    assert widget.last_result.imported_count == 0
    assert widget.table.rowCount() == 1
    assert widget.table.item(0, 1).text() == "market_2026-08-05.csv"
    assert widget.card_discovered.value_label.text() == "1"

    # Verify destination CSV was NOT created during dry run
    dest_csv = tmp_path / "data" / "raw" / "market_2026-08-05.csv"
    assert not dest_csv.exists()


def test_worker_completion_restores_controls_and_cleans_up(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    source_dir = tmp_path / "source_archive"
    create_legacy_csv(source_dir / "market_2026-08-05.csv", "2026-08-05")

    widget = ImportWidget(repo)
    widget.txt_source.setText(str(source_dir))

    widget.run_import(dry_run=True)

    # While running, controls must be disabled and progress bar visible
    assert widget.btn_dry_run.isEnabled() is False
    assert widget.btn_apply.isEnabled() is False
    assert widget.txt_source.isEnabled() is False
    assert widget.btn_browse.isEnabled() is False
    assert widget.chk_recursive.isEnabled() is False

    worker_ref = widget.active_worker
    assert worker_ref is not None

    worker_ref.wait(5000)
    qapp.processEvents()

    # After completion:
    # 1. controls are re-enabled
    assert widget.btn_dry_run.isEnabled() is True
    assert widget.btn_apply.isEnabled() is True
    assert widget.txt_source.isEnabled() is True
    assert widget.btn_browse.isEnabled() is True
    assert widget.chk_recursive.isEnabled() is True

    # 2. running status is replaced with completed status
    assert not widget.lbl_status.text().startswith("Running")
    assert "finished in" in widget.lbl_status.text()

    # 3. progress bar is hidden
    assert widget.progress_bar.isVisible() is False

    # 4. worker reference is cleaned up
    assert widget.active_worker is None


def test_worker_error_restores_controls_and_cleans_up(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    source_dir = tmp_path / "source_archive"
    source_dir.mkdir(parents=True)

    widget = ImportWidget(repo)
    widget.txt_source.setText(str(source_dir))

    with patch("psx_data_sync.gui.import_panel.import_local_csv_directory", side_effect=RuntimeError("Synthetic import failure")):
        widget.run_import(dry_run=True)

        worker_ref = widget.active_worker
        assert worker_ref is not None

        worker_ref.wait(5000)
        qapp.processEvents()

    # After error:
    # 1. controls are re-enabled
    assert widget.btn_dry_run.isEnabled() is True
    assert widget.btn_apply.isEnabled() is True

    # 2. error status is displayed
    assert widget.lbl_status.text() == "Import failed due to error."
    assert "Synthetic import failure" in widget.error_label.text()

    # 3. progress bar is hidden
    assert widget.progress_bar.isVisible() is False

    # 4. worker reference is cleaned up
    assert widget.active_worker is None


def test_apply_import_cancellation_aborts_action(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    source_dir = tmp_path / "source_archive"
    create_legacy_csv(source_dir / "market_2026-08-05.csv", "2026-08-05")

    widget = ImportWidget(repo)
    widget.txt_source.setText(str(source_dir))

    with patch.object(QMessageBox, "question", return_value=QMessageBox.StandardButton.No):
        widget.run_import(dry_run=False)

    assert widget.lbl_status.text() == "Import cancelled by user."
    assert widget.last_result is None


def test_apply_import_with_confirmation_and_callback(qapp: QApplication, tmp_path: Path) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    source_dir = tmp_path / "source_archive"
    create_legacy_csv(source_dir / "market_2026-08-05.csv", "2026-08-05")

    callback_mock = MagicMock()
    widget = ImportWidget(repo, on_import_success=callback_mock)
    widget.txt_source.setText(str(source_dir))

    with patch.object(QMessageBox, "question", return_value=QMessageBox.StandardButton.Yes):
        widget.run_import(dry_run=False)

    if widget.active_worker:
        widget.active_worker.wait(5000)
        qapp.processEvents()

    assert widget.last_result is not None
    assert widget.last_result.dry_run is False
    assert widget.last_result.imported_count == 1

    dest_csv = tmp_path / "data" / "raw" / "market_2026-08-05.csv"
    assert dest_csv.exists()

    callback_mock.assert_called_once()
