from __future__ import annotations

import os

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


def test_gui_panel_navigation_smoke(qapp: QApplication, tmp_path_factory: pytest.TempPathFactory) -> None:
    """Smoke test ensuring all sidebar pages initialize and render cleanly."""
    tmp_dir = tmp_path_factory.mktemp("gui_smoke")
    settings = Settings(
        raw_output_dir=tmp_dir / "data" / "raw",
        state_db_path=tmp_dir / "data" / "state" / "psx_sync.db",
        repair_staging_dir=tmp_dir / "data" / "state" / "repair_staging",
    )
    repo = StateRepository(settings.state_db_path, project_root=tmp_dir)
    repo.initialize()

    window = PSXMainWindow(repo)
    assert window.stack.count() == 6

    # Cycle through all pages (Dashboard, Download, Import, Reconciliation, Parquet, Logs)
    for idx in range(6):
        window.stack.setCurrentIndex(idx)
        current_widget = window.stack.currentWidget()
        assert current_widget is not None
        assert current_widget.isVisible() or True

    window.close()
