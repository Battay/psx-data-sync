from __future__ import annotations

import os
from pathlib import Path

import pytest
from PySide6.QtWidgets import QApplication

from psx_data_sync.config import Settings
from psx_data_sync.gui.app import create_app
from psx_data_sync.gui.main_window import PSXMainWindow
from psx_data_sync.state_db import StateRepository

os.environ["QT_QPA_PLATFORM"] = "offscreen"


@pytest.fixture(scope="session")
def qapp() -> QApplication:
    existing = QApplication.instance()
    if existing is None:
        return create_app(["--offscreen"])
    if not isinstance(existing, QApplication):
        raise RuntimeError("Existing Qt application is not a QApplication")
    return existing


def test_first_run_directory_creation_and_gui_init(tmp_path: Path, qapp: QApplication) -> None:
    """Verify first-run initialization when target data directory does not yet exist."""
    non_existent_base = tmp_path / "NonExistentAppSupport" / "PSX Data Sync"
    db_path = non_existent_base / "data" / "state" / "psx_sync.db"
    raw_dir = non_existent_base / "data" / "raw"
    repair_dir = non_existent_base / "data" / "state" / "repair_staging"

    assert not non_existent_base.exists()

    settings = Settings(
        raw_output_dir=raw_dir,
        state_db_path=db_path,
        repair_staging_dir=repair_dir,
    )

    repository = StateRepository(
        settings.state_db_path,
        project_root=non_existent_base,
        source_endpoint=settings.historical_url,
    )
    repository.initialize()

    # Verify directory structure was created safely
    assert db_path.exists()
    assert db_path.parent.exists()

    # Verify main window initializes cleanly with empty database
    window = PSXMainWindow(repository)
    assert window.windowTitle() == "PSX Data Sync"
    assert window.nav_list.count() == 6

    # Verify dashboard summary returns clean empty metrics without errors
    summary = repository.get_dashboard_summary()
    assert summary.total_tracked_dates == 0
    assert summary.verified_trading_count == 0

    window.close()
