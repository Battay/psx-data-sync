from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

import pytest
from PySide6.QtWidgets import QApplication

from psx_data_sync.exporter import canonical_csv_bytes
from psx_data_sync.gui.app import create_app
from psx_data_sync.gui.dashboard import DashboardWidget
from psx_data_sync.state import (
    DownloadAttemptEvent,
    DownloadResult,
    DownloadStatus,
    ParquetExportStatus,
    ValidEquityRow,
)
from psx_data_sync.state_db import StateRepository

os.environ["QT_QPA_PLATFORM"] = "offscreen"


@pytest.fixture(scope="session")
def qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = create_app(["--offscreen"])
    return app


def _row(symbol: str = "AAA") -> ValidEquityRow:
    return ValidEquityRow(
        row_index=1,
        symbol=symbol,
        ldcp=Decimal("10.0"),
        open=Decimal("10.0"),
        high=Decimal("10.0"),
        low=Decimal("10.0"),
        close=Decimal("10.0"),
        change=Decimal("0.0"),
        change_percent=Decimal("0.0"),
        volume=100,
    )


def test_dashboard_with_empty_database(qapp: QApplication, tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    repo = StateRepository(db_path, project_root=tmp_path)
    repo.initialize()

    dash = DashboardWidget(repo)
    assert dash.summary is not None
    assert dash.summary.total_tracked_dates == 0
    assert dash.summary.earliest_date is None
    assert dash.summary.latest_date is None
    assert dash.lbl_date_range.text() == "No tracked dates"
    assert dash.card_total_tracked.value_label.text() == "0"
    assert dash.card_verified_trading.value_label.text() == "0"
    assert dash.error_label.isVisible() is False


def test_dashboard_displays_seeded_repository_counts(
    qapp: QApplication, tmp_path: Path
) -> None:
    db_path = tmp_path / "state.db"
    repo = StateRepository(db_path, project_root=tmp_path)
    repo.initialize()

    # Seed local CSV verified date
    raw_dir = tmp_path / "data" / "raw"
    csv1 = raw_dir / "market_2026-08-05.csv"
    csv1.parent.mkdir(parents=True, exist_ok=True)
    csv1.write_bytes(canonical_csv_bytes((_row("AAA"),)))
    repo.index_local_file(csv1)

    # Seed download result date
    csv2 = raw_dir / "market_2026-08-06.csv"
    csv2.write_bytes(canonical_csv_bytes((_row("BBB"),)))
    run_id = repo.begin_sync_run("fetch", "2026-08-06", "2026-08-06", 1, 1)
    repo.record_attempt(
        run_id,
        DownloadAttemptEvent(
            requested_date="2026-08-06",
            attempt_number=1,
            started_at="2026-08-06T10:00:00+00:00",
            finished_at="2026-08-06T10:00:01+00:00",
            duration_ms=1000.0,
            http_status=200,
            response_bytes=100,
            response_classification="EQUITY_ROWS",
            final_status=DownloadStatus.TRADING_DATA,
            retryable=False,
            parsed_row_count=1,
            valid_row_count=1,
            checksum="abc",
            saved_path=csv2,
        ),
    )
    repo.record_download_result(
        run_id,
        DownloadResult(
            requested_date="2026-08-06",
            status=DownloadStatus.TRADING_DATA,
            attempts=1,
            valid_row_count=1,
            checksum="abc",
            saved_path=csv2,
        ),
    )

    # Seed Parquet export record
    state_05 = repo.get_date_state("2026-08-05")
    assert state_05 is not None and state_05.csv_checksum_sha256 is not None
    repo.upsert_parquet_export(
        "2026-08-05",
        status=ParquetExportStatus.CURRENT,
        schema_version="v1",
        source_csv_checksum_sha256=state_05.csv_checksum_sha256,
        source_row_count=1,
        parquet_path=tmp_path / "data" / "parquet" / "market_2026-08-05.parquet",
        parquet_checksum_sha256="pq1",
        parquet_row_count=1,
        verified_at="2026-08-05T12:00:00+00:00",
    )

    dash = DashboardWidget(repo)
    assert dash.summary is not None
    assert dash.summary.total_tracked_dates == 2
    assert dash.summary.earliest_date == "2026-08-05"
    assert dash.summary.latest_date == "2026-08-06"
    assert dash.summary.verified_trading_count == 2
    assert dash.summary.local_csv_verified_count == 1
    assert dash.summary.parquet_current_count == 1
    assert dash.summary.total_canonical_csv_count == 2

    assert dash.card_total_tracked.value_label.text() == "2"
    assert dash.card_verified_trading.value_label.text() == "2"
    assert dash.card_local_csv.value_label.text() == "1"
    assert dash.card_pq_current.value_label.text() == "1"
    assert dash.card_canonical_csv.value_label.text() == "2"


def test_dashboard_refresh_does_not_mutate_db(
    qapp: QApplication, tmp_path: Path
) -> None:
    db_path = tmp_path / "state.db"
    repo = StateRepository(db_path, project_root=tmp_path)
    repo.initialize()

    dash = DashboardWidget(repo)
    summary_before = repo.get_dashboard_summary()

    dash.refresh_dashboard()
    dash.refresh_dashboard()

    summary_after = repo.get_dashboard_summary()
    assert summary_before == summary_after


def test_dashboard_error_handling_resilience(
    qapp: QApplication, tmp_path: Path
) -> None:
    db_path = tmp_path / "non_existent_dir" / "state.db"
    repo = StateRepository(db_path, project_root=tmp_path)

    # Repository not initialized, get_dashboard_summary will raise exception
    dash = DashboardWidget(repo)
    assert "Dashboard Error" in dash.error_label.text()
