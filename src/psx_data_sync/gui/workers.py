"""Reusable Qt worker and threading infrastructure."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from PySide6.QtCore import QObject, QThread, Signal

logger = logging.getLogger(__name__)


class WorkerSignals(QObject):
    """Signals emitted by background workers."""

    started = Signal()
    result = Signal(object)
    error = Signal(str)
    finished = Signal()


class BaseWorker(QThread):
    """Generic thread worker executing a target function asynchronously."""

    def __init__(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

    def run(self) -> None:
        self.signals.started.emit()
        try:
            res = self.func(*self.args, **self.kwargs)
            self.signals.result.emit(res)
        except Exception as exc:
            logger.exception("background worker execution failed")
            self.signals.error.emit(str(exc))
        finally:
            self.signals.finished.emit()
