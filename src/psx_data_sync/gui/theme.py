"""Centralized GUI dark theme and QSS styling module for PSX Data Sync."""

from __future__ import annotations

import logging
from typing import Any

from PySide6.QtWidgets import QApplication, QWidget

logger = logging.getLogger(__name__)

# Dark Theme Color Tokens
BG_DARK = "#0f172a"          # Slate 900 main window background
SURFACE_DARK = "#1e293b"     # Slate 800 sidebar & panel background
CARD_BG = "#1e293b"          # Slate 800 card background
CARD_BORDER = "#334155"      # Slate 700 borders
PRIMARY_ACCENT = "#3b82f6"   # Blue 500 primary accent
PRIMARY_HOVER = "#2563eb"    # Blue 600 hover state
TEXT_PRIMARY = "#f8fafc"     # Slate 50 text primary
TEXT_MUTED = "#94a3b8"       # Slate 400 text secondary/muted

DARK_THEME_QSS = """
/* Base Window & Widget Backgrounds */
QMainWindow, QDialog {
    background-color: #0f172a;
    color: #f8fafc;
}

QWidget {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    color: #f8fafc;
}

/* Sidebar List Widget */
QListWidget {
    background-color: #161e2e;
    border: none;
    border-right: 1px solid #1e293b;
    outline: none;
    padding: 6px;
}

QListWidget::item {
    color: #94a3b8;
    padding: 10px 14px;
    border-radius: 6px;
    font-size: 14px;
    font-weight: 500;
    margin-bottom: 2px;
}

QListWidget::item:hover {
    background-color: #1e293b;
    color: #f8fafc;
}

QListWidget::item:selected {
    background-color: #3b82f6;
    color: #ffffff;
    font-weight: 600;
}

/* Group Boxes / Card Containers */
QGroupBox {
    background-color: #1e293b;
    border: 1px solid #334155;
    border-radius: 8px;
    margin-top: 16px;
    font-weight: 600;
    font-size: 13px;
    color: #94a3b8;
    padding-top: 16px;
}

QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;
    left: 12px;
    padding: 0 6px;
    background-color: #1e293b;
    color: #3b82f6;
}

/* Push Buttons */
QPushButton {
    background-color: #334155;
    color: #f8fafc;
    border: 1px solid #475569;
    border-radius: 6px;
    padding: 7px 16px;
    font-size: 13px;
    font-weight: 500;
}

QPushButton:hover {
    background-color: #475569;
    border-color: #64748b;
}

QPushButton:pressed {
    background-color: #1e293b;
}

QPushButton:disabled {
    background-color: #1e293b;
    color: #475569;
    border-color: #334155;
}

/* Primary Accent Buttons (Apply / Download / Main actions) */
QPushButton[accent="true"] {
    background-color: #3b82f6;
    color: #ffffff;
    border: 1px solid #2563eb;
}

QPushButton[accent="true"]:hover {
    background-color: #2563eb;
    border-color: #1d4ed8;
}

QPushButton[accent="true"]:disabled {
    background-color: #1e293b;
    color: #475569;
    border-color: #334155;
}

/* Inputs & Spinboxes */
QLineEdit, QSpinBox {
    background-color: #0f172a;
    color: #f8fafc;
    border: 1px solid #334155;
    border-radius: 6px;
    padding: 6px 10px;
    font-size: 13px;
}

QLineEdit:focus, QSpinBox:focus {
    border: 1px solid #3b82f6;
}

QLineEdit:disabled, QSpinBox:disabled {
    background-color: #1e293b;
    color: #64748b;
}

/* Checkboxes */
QCheckBox {
    color: #f8fafc;
    font-size: 13px;
    spacing: 8px;
}

QCheckBox::indicator {
    width: 16px;
    height: 16px;
    border-radius: 4px;
    border: 1px solid #475569;
    background-color: #0f172a;
}

QCheckBox::indicator:checked {
    background-color: #3b82f6;
    border-color: #2563eb;
}

/* Tables */
QTableWidget {
    background-color: #0f172a;
    alternate-background-color: #161e2e;
    color: #f8fafc;
    gridline-color: #1e293b;
    border: 1px solid #334155;
    border-radius: 6px;
    font-size: 13px;
}

QTableWidget::item {
    padding: 6px 8px;
}

QTableWidget::item:selected {
    background-color: #2563eb;
    color: #ffffff;
}

QHeaderView::section {
    background-color: #1e293b;
    color: #94a3b8;
    padding: 8px;
    border: none;
    border-bottom: 1px solid #334155;
    font-weight: 600;
    font-size: 12px;
}

/* Progress Bar */
QProgressBar {
    background-color: #0f172a;
    border: 1px solid #334155;
    border-radius: 4px;
    height: 12px;
    text-align: center;
}

QProgressBar::chunk {
    background-color: #3b82f6;
    border-radius: 3px;
}

/* Scrollbars */
QScrollBar:vertical, QScrollBar:horizontal {
    background-color: #0f172a;
    border: none;
    width: 10px;
    height: 10px;
}

QScrollBar::handle:vertical, QScrollBar::handle:horizontal {
    background-color: #334155;
    border-radius: 5px;
}

QScrollBar::handle:vertical:hover, QScrollBar::handle:horizontal:hover {
    background-color: #475569;
}
"""


def apply_dark_theme(target: QApplication | QWidget) -> None:
    """Apply the centralized dark QSS stylesheet to a QApplication or top-level QWidget."""

    target.setStyleSheet(DARK_THEME_QSS)
