from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

import psx_data_sync.cli as cli
from psx_data_sync.state import DownloadResult, DownloadStatus


runner = CliRunner()


def test_help_lists_fetch_command() -> None:
    result = runner.invoke(cli.app, ["--help"])

    assert result.exit_code == 0
    assert "fetch" in result.output


def test_invalid_date_has_distinct_exit_code(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PSX_RAW_OUTPUT_DIR", str(tmp_path))

    result = runner.invoke(cli.app, ["fetch", "--date", "2026-8-5"])

    assert result.exit_code == 2
    assert "INVALID_DATE" in result.output
    assert "YYYY-MM-DD" in result.output


def test_mocked_success_prints_summary(monkeypatch, tmp_path: Path) -> None:
    expected = DownloadResult(
        requested_date="2026-08-05",
        status=DownloadStatus.TRADING_DATA,
        http_status=200,
        attempts=2,
        response_bytes=2048,
        parsed_row_count=2,
        valid_row_count=2,
        elapsed_ms=123,
        saved_path=tmp_path / "market_2026-08-05.csv",
        checksum="abc123",
    )
    monkeypatch.setattr(cli, "run_download", lambda _: expected)

    result = runner.invoke(cli.app, ["fetch", "-d", "2026-08-05"])

    assert result.exit_code == 0
    assert "TRADING_DATA" in result.output
    assert "abc123" in result.output


def test_mocked_failure_returns_nonzero(monkeypatch) -> None:
    expected = DownloadResult(
        requested_date="2026-08-05",
        status=DownloadStatus.TEMPORARY_FAILURE,
        attempts=3,
        error="timed out",
    )
    monkeypatch.setattr(cli, "run_download", lambda _: expected)

    result = runner.invoke(cli.app, ["fetch", "--date", "2026-08-05"])

    assert result.exit_code == 1
    assert "TEMPORARY_FAILURE" in result.output
    assert "timed out" in result.output
