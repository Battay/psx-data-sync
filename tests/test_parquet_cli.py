from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from psx_data_sync.cli import app
from psx_data_sync.state_db import StateRepository
from tests.test_parquet_sync import seed_verified_date

runner = CliRunner()


def setup_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> StateRepository:
    db_path = tmp_path / "data" / "state" / "psx_sync.db"
    raw_dir = tmp_path / "data" / "raw"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("PSX_STATE_DB_PATH", str(db_path))
    monkeypatch.setenv("PSX_RAW_OUTPUT_DIR", str(raw_dir))
    repo = StateRepository(db_path, project_root=tmp_path, raw_output_dir=raw_dir)
    repo.initialize()
    return repo


def test_help_describes_full_consolidated_export_without_ranges() -> None:
    result = runner.invoke(app, ["export-parquet", "--help"])
    assert result.exit_code == 0
    assert "one Parquet file" in result.output
    assert "--apply" in result.output
    assert "--rebuild" in result.output
    assert "--start" not in result.output
    assert "--end" not in result.output


def test_default_dry_run_reports_missing_and_creates_no_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = setup_env(tmp_path, monkeypatch)
    seed_verified_date(repo)
    result = runner.invoke(app, ["export-parquet"])

    assert result.exit_code == 0
    assert "DRY_RUN (planning only)" in result.output
    assert "Status:" in result.output
    assert "MISSING" in result.output
    assert not (tmp_path / "data" / "parquet" / "market.parquet").exists()


def test_apply_creates_consolidated_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = setup_env(tmp_path, monkeypatch)
    seed_verified_date(repo)
    result = runner.invoke(app, ["export-parquet", "--apply"])

    assert result.exit_code == 0
    assert "APPLY (actual export)" in result.output
    assert "CURRENT" in result.output
    assert "Rows written:" in result.output
    assert (tmp_path / "data" / "parquet" / "market.parquet").exists()


def test_rebuild_requires_apply(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    setup_env(tmp_path, monkeypatch)
    result = runner.invoke(app, ["export-parquet", "--rebuild"])
    assert result.exit_code == 2
    assert "--rebuild requires --apply" in result.output

    contradictory = runner.invoke(
        app, ["export-parquet", "--apply", "--dry-run", "--rebuild"]
    )
    assert contradictory.exit_code == 2


def test_json_report_has_required_consolidated_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = setup_env(tmp_path, monkeypatch)
    seed_verified_date(repo)
    result = runner.invoke(app, ["export-parquet", "--json"])

    assert result.exit_code == 0
    data = json.loads(result.output)
    for key in (
        "source_dates", "source_rows", "status", "rows_written", "output_path",
        "source_identity", "legacy_partition_count", "errors",
    ):
        assert key in data
    assert data["mode"] == "DRY_RUN"
    assert data["source_dates"] == 1
    assert data["status"] == "MISSING"


def test_canonical_csv_remains_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = setup_env(tmp_path, monkeypatch)
    csv_path = seed_verified_date(repo)
    before = csv_path.read_bytes()
    result = runner.invoke(app, ["export-parquet", "--apply"])
    assert result.exit_code == 0
    assert csv_path.read_bytes() == before


def test_invalid_verified_source_exits_three(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = setup_env(tmp_path, monkeypatch)
    csv_path = seed_verified_date(repo)
    csv_path.write_bytes(b"tampered")

    result = runner.invoke(app, ["export-parquet", "--json"])
    assert result.exit_code == 3
    data = json.loads(result.output)
    assert data["errors"]
