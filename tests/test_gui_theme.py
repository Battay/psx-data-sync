from __future__ import annotations

import os
from pathlib import Path

import pytest
from PySide6.QtWidgets import QApplication

from psx_data_sync.gui.app import create_app
from psx_data_sync.gui.main_window import PSXMainWindow
from psx_data_sync.gui.theme import DARK_THEME_QSS, apply_dark_theme
from psx_data_sync.state_db import StateRepository

os.environ["QT_QPA_PLATFORM"] = "offscreen"


@pytest.fixture(scope="session")
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = create_app(["--offscreen"])
    return app


def test_theme_qss_string_not_empty() -> None:
    assert isinstance(DARK_THEME_QSS, str)
    assert "QMainWindow" in DARK_THEME_QSS
    assert "#0f172a" in DARK_THEME_QSS
    assert "#3b82f6" in DARK_THEME_QSS


def test_apply_dark_theme_sets_stylesheet(qapp: QApplication) -> None:
    apply_dark_theme(qapp)
    assert qapp.styleSheet() == DARK_THEME_QSS


def test_main_window_construction_under_dark_theme(
    qapp: QApplication, tmp_path: Path
) -> None:
    repo = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repo.initialize()

    window = PSXMainWindow(repo)
    assert window.minimumWidth() >= 1100
    assert window.minimumHeight() >= 720
    assert window.nav_list.count() == 6
