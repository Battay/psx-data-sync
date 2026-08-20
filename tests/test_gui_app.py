from __future__ import annotations

import os
from pathlib import Path

import pytest
from PySide6.QtWidgets import QApplication

from psx_data_sync.gui.app import create_app, main
from psx_data_sync.gui.main_window import PSXMainWindow
from psx_data_sync.state_db import StateRepository

os.environ["QT_QPA_PLATFORM"] = "offscreen"


@pytest.fixture(scope="session")
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = create_app(["--offscreen"])
    return app


def test_qapplication_initializes(qapp: QApplication) -> None:
    assert qapp is not None
    assert isinstance(qapp, QApplication)


def test_main_window_constructs_and_closes(qapp: QApplication, tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    repo = StateRepository(db_path, project_root=tmp_path)
    repo.initialize()

    window = PSXMainWindow(repo)
    assert window.windowTitle() == "PSX Data Sync"
    assert window.stack.count() == 6
    window.close()


def test_app_main_non_exec(qapp: QApplication, tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    repo = StateRepository(db_path, project_root=tmp_path)
    repo.initialize()

    exit_code = main(repository=repo, exec_app=False)
    assert exit_code == 0
