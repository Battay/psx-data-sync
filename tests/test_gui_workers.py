from __future__ import annotations

import os
from pathlib import Path

import pytest
from PySide6.QtWidgets import QApplication

from psx_data_sync.gui.app import create_app
from psx_data_sync.gui.workers import BaseWorker, WorkerSignals

os.environ["QT_QPA_PLATFORM"] = "offscreen"


@pytest.fixture(scope="session")
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = create_app(["--offscreen"])
    return app


def test_worker_signals_instance(qapp: QApplication) -> None:
    signals = WorkerSignals()
    assert signals is not None


def test_base_worker_successful_execution(qapp: QApplication) -> None:
    started_called = []
    result_value = []
    error_called = []
    finished_called = []

    def sample_func(a: int, b: int) -> int:
        return a + b

    worker = BaseWorker(sample_func, 10, 20)
    worker.signals.started.connect(lambda: started_called.append(True))
    worker.signals.result.connect(lambda val: result_value.append(val))
    worker.signals.error.connect(lambda err: error_called.append(err))
    worker.signals.finished.connect(lambda: finished_called.append(True))

    worker.start()
    worker.wait(2000)
    qapp.processEvents()

    assert len(started_called) == 1
    assert result_value == [30]
    assert len(error_called) == 0
    assert len(finished_called) == 1


def test_base_worker_failing_execution(qapp: QApplication) -> None:
    started_called = []
    result_value = []
    error_called = []
    finished_called = []

    def failing_func() -> None:
        raise ValueError("synthetic worker failure")

    worker = BaseWorker(failing_func)
    worker.signals.started.connect(lambda: started_called.append(True))
    worker.signals.result.connect(lambda val: result_value.append(val))
    worker.signals.error.connect(lambda err: error_called.append(err))
    worker.signals.finished.connect(lambda: finished_called.append(True))

    worker.start()
    worker.wait(2000)
    qapp.processEvents()

    assert len(started_called) == 1
    assert len(result_value) == 0
    assert len(error_called) == 1
    assert "synthetic worker failure" in error_called[0]
    assert len(finished_called) == 1
