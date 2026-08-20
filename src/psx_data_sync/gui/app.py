"""GUI Application Entrypoint (`python -m psx_data_sync.gui.app`)."""

from __future__ import annotations

import sys
from typing import Sequence

from PySide6.QtWidgets import QApplication

from psx_data_sync.config import Settings
from psx_data_sync.gui.main_window import PSXMainWindow
from psx_data_sync.gui.theme import apply_dark_theme
from psx_data_sync.state_db import StateRepository


def create_app(args: Sequence[str] | None = None) -> QApplication:
    """Create or retrieve a QApplication instance safely."""

    app = QApplication.instance()
    if app is None:
        sys_args = list(args) if args is not None else sys.argv
        app = QApplication(sys_args)
        apply_dark_theme(app)
    return app


def main(
    repository: StateRepository | None = None,
    args: Sequence[str] | None = None,
    exec_app: bool = True,
) -> int:
    """Initialize and run the desktop GUI application."""

    app = create_app(args)

    if repository is None:
        settings = Settings.from_env()
        raw_dir = settings.raw_output_dir.resolve()
        project_root = (
            raw_dir.parent.parent
            if raw_dir.name == "raw" and raw_dir.parent.name == "data"
            else raw_dir.parent
        )
        repository = StateRepository(
            settings.state_db_path,
            project_root=project_root,
            source_endpoint=settings.historical_url,
        )
        repository.initialize()

    window = PSXMainWindow(repository)
    window.show()

    if exec_app:
        return app.exec()
    return 0


if __name__ == "__main__":
    sys.exit(main())
