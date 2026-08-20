"""Custom QDateEdit control supporting ISO typing and calendar popups."""

from __future__ import annotations

from datetime import date

from PySide6.QtCore import QDate, Qt
from PySide6.QtWidgets import QCalendarWidget, QDateEdit, QWidget


class PSXDateEdit(QDateEdit):
    """QDateEdit control with ISO yyyy-MM-dd formatting, calendar popup, and string helpers."""

    def __init__(
        self,
        initial_date: str | date | QDate | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setDisplayFormat("yyyy-MM-dd")
        self.setCalendarPopup(True)
        self.setFixedWidth(130)

        cal = self.calendarWidget()
        if cal is not None:
            cal.setVerticalHeaderFormat(QCalendarWidget.VerticalHeaderFormat.NoVerticalHeader)
            cal.clicked.connect(self._on_calendar_date_clicked)

        if initial_date is not None:
            self.set_date_val(initial_date)
        else:
            self.setDate(QDate.currentDate())

    def _on_calendar_date_clicked(self, qdate: QDate) -> None:
        """Update widget date on single-click and close calendar popup."""
        self.setDate(qdate)
        cal = self.calendarWidget()
        if cal is not None:
            popup_container = cal.parentWidget()
            if popup_container is not None:
                popup_container.hide()

    @property
    def date_str(self) -> str:
        """Return date as ISO string 'YYYY-MM-DD'."""
        qdate = self.date()
        return f"{qdate.year():04d}-{qdate.month():02d}-{qdate.day():02d}"

    def set_date_val(self, val: str | date | QDate) -> None:
        """Set date from string 'YYYY-MM-DD', datetime.date, or QDate."""
        if isinstance(val, str):
            val_clean = val.strip()
            parsed = QDate.fromString(val_clean, Qt.DateFormat.ISODate)
            if parsed.isValid():
                self.setDate(parsed)
            else:
                d = date.fromisoformat(val_clean)
                self.setDate(QDate(d.year, d.month, d.day))
        elif isinstance(val, date):
            self.setDate(QDate(val.year, val.month, val.day))
        elif isinstance(val, QDate):
            self.setDate(val)
        else:
            raise TypeError(f"Unsupported date value type: {type(val)}")
