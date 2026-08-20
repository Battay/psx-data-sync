from __future__ import annotations

import os
from datetime import date

import pytest
from PySide6.QtCore import QDate
from PySide6.QtWidgets import QApplication, QCalendarWidget

from psx_data_sync.gui.app import create_app
from psx_data_sync.gui.widgets.date_edit import PSXDateEdit

os.environ["QT_QPA_PLATFORM"] = "offscreen"


@pytest.fixture(scope="session")
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = create_app(["--offscreen"])
    return app


def test_psx_date_edit_properties(qapp: QApplication) -> None:
    edit = PSXDateEdit("2026-08-15")

    assert edit.calendarPopup() is True
    assert edit.displayFormat() == "yyyy-MM-dd"
    assert edit.date_str == "2026-08-15"

    cal = edit.calendarWidget()
    assert cal is not None
    assert cal.verticalHeaderFormat() == QCalendarWidget.VerticalHeaderFormat.NoVerticalHeader


def test_psx_date_edit_set_date_val(qapp: QApplication) -> None:
    edit = PSXDateEdit("2026-08-01")

    # Set via string
    edit.set_date_val("2026-08-20")
    assert edit.date_str == "2026-08-20"

    # Set via datetime.date
    edit.set_date_val(date(2026, 9, 1))
    assert edit.date_str == "2026-09-01"

    # Set via QDate
    edit.set_date_val(QDate(2026, 10, 5))
    assert edit.date_str == "2026-10-05"


def test_single_click_calendar_selection(qapp: QApplication) -> None:
    edit = PSXDateEdit("2026-08-01")
    cal = edit.calendarWidget()
    assert cal is not None

    # Emit single-click signal on calendar
    new_date = QDate(2026, 8, 25)
    cal.clicked.emit(new_date)

    assert edit.date_str == "2026-08-25"
