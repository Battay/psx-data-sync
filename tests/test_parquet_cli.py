from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from psx_data_sync.cli import app
from psx_data_sync.state import (
    DownloadAttemptEvent,
    DownloadStatus,
)
from psx_data_sync.state_db import StateRepository
from tests.test_parquet_sync import seed_verified_date

runner = CliRunner()


def setup_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[StateRepository, Path]:
    db_path = tmp_path / "data" / "state" / "psx_sync.db"
    raw_dir = tmp_path / "data" / "raw"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("PSX_STATE_DB_PATH", str(db_path))
    monkeypatch.setenv("PSX_RAW_OUTPUT_DIR", str(raw_dir))

    repo = StateRepository(db_path, project_root=tmp_path)
    repo.initialize()
    return repo, raw_dir


def test_default_dry_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, _ = setup_env(tmp_path, monkeypatch)
    seed_verified_date(repo, "2026-08-07")

    result = runner.invoke(
        app,
        ["export-parquet", "--start", "2026-08-07", "--end", "2026-08-07"],
    )

    assert result.exit_code == 0
    assert "DRY_RUN (planning only)" in result.output
    assert repo.get_parquet_export("2026-08-07") is None


def test_apply_mode_creates_partition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, _ = setup_env(tmp_path, monkeypatch)
    seed_verified_date(repo, "2026-08-07")

    result = runner.invoke(
        app,
        [
            "export-parquet",
            "--start",
            "2026-08-07",
            "--end",
            "2026-08-07",
            "--apply",
        ],
    )

    assert result.exit_code == 0
    assert "APPLY (actual export)" in result.output
    assert "Written / rebuilt:" in result.output
    assert "1" in result.output

    db_rec = repo.get_parquet_export("2026-08-07")
    assert db_rec is not None
    assert db_rec.status.value == "CURRENT"


def test_rebuild_requires_apply(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup_env(tmp_path, monkeypatch)

    result = runner.invoke(
        app,
        [
            "export-parquet",
            "--start",
            "2026-08-07",
            "--end",
            "2026-08-07",
            "--rebuild",
        ],
    )

    assert result.exit_code == 2
    assert "--rebuild requires --apply" in result.output


def test_json_output_parses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, _ = setup_env(tmp_path, monkeypatch)
    seed_verified_date(repo, "2026-08-07")

    result = runner.invoke(
        app,
        [
            "export-parquet",
            "--start",
            "2026-08-07",
            "--end",
            "2026-08-07",
            "--json",
        ],
    )

    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["mode"] == "DRY_RUN"
    assert data["requested_count"] == 1
    assert data["synchronized"] is True
    assert len(data["results"]) == 1
    assert data["results"][0]["action"] == "CREATE"


def test_second_apply_reports_no_op(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, _ = setup_env(tmp_path, monkeypatch)
    seed_verified_date(repo, "2026-08-07")

    res1 = runner.invoke(
        app,
        [
            "export-parquet",
            "--start",
            "2026-08-07",
            "--end",
            "2026-08-07",
            "--apply",
        ],
    )
    assert res1.exit_code == 0

    res2 = runner.invoke(
        app,
        [
            "export-parquet",
            "--start",
            "2026-08-07",
            "--end",
            "2026-08-07",
            "--apply",
        ],
    )
    assert res2.exit_code == 0
    assert "Current (no-op):" in res2.output


def test_incomplete_range_exit_code_3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup_env(tmp_path, monkeypatch)
    # Date 2026-08-07 is NEVER_ATTEMPTED (unresolved)

    result = runner.invoke(
        app,
        ["export-parquet", "--start", "2026-08-07", "--end", "2026-08-07"],
    )

    assert result.exit_code == 3
    assert "Synchronized:" in result.output
    assert "No" in result.output
    assert "Excluded unresolved:" in result.output


def test_confirmed_non_trading_presentation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, _ = setup_env(tmp_path, monkeypatch)

    run_id = repo.begin_sync_run("fetch", "2026-08-09", "2026-08-09", 1, 1)
    repo.record_attempt(
        run_id,
        DownloadAttemptEvent(
            requested_date="2026-08-09",
            attempt_number=1,
            started_at="2026-08-09T10:00:00+00:00",
            finished_at="2026-08-09T10:00:01+00:00",
            duration_ms=500.0,
            http_status=200,
            response_bytes=100,
            response_classification="EMPTY_MARKET_RESPONSE",
            final_status=DownloadStatus.CONFIRMED_NON_TRADING,
            retryable=False,
        ),
    )

    result = runner.invoke(
        app,
        ["export-parquet", "--start", "2026-08-09", "--end", "2026-08-09"],
    )

    assert result.exit_code == 0
    assert "Excluded non-trading:" in result.output
    assert "Synchronized:" in result.output
    assert "Yes" in result.output


def test_invalid_date_range_handling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup_env(tmp_path, monkeypatch)

    result = runner.invoke(
        app,
        ["export-parquet", "--start", "invalid-date", "--end", "2026-08-07"],
    )

    assert result.exit_code == 2
    assert "Input error" in result.output


def test_canonical_csv_remains_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, _ = setup_env(tmp_path, monkeypatch)
    csv_path = seed_verified_date(repo, "2026-08-07")
    content_before = csv_path.read_bytes()

    runner.invoke(
        app,
        [
            "export-parquet",
            "--start",
            "2026-08-07",
            "--end",
            "2026-08-07",
            "--apply",
        ],
    )

    assert csv_path.read_bytes() == content_before
