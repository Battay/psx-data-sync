from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from psx_data_sync.cli import app
from psx_data_sync.state_db import StateRepository
from tests.test_importer import _row, create_sample_csv

runner = CliRunner()


def setup_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[StateRepository, Path, Path]:
    db_path = tmp_path / "data" / "state" / "psx_sync.db"
    raw_dir = tmp_path / "data" / "raw"
    source_dir = tmp_path / "virtual_trader_archive"

    db_path.parent.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    source_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("PSX_STATE_DB_PATH", str(db_path))
    monkeypatch.setenv("PSX_RAW_OUTPUT_DIR", str(raw_dir))

    repo = StateRepository(db_path, project_root=tmp_path)
    repo.initialize()
    return repo, raw_dir, source_dir


def test_default_dry_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, raw_dir, source_dir = setup_env(tmp_path, monkeypatch)
    source_csv = source_dir / "market_2016-07-26.csv"
    create_sample_csv(source_csv)

    result = runner.invoke(app, ["import-csv", "--source", str(source_dir)])

    assert result.exit_code == 0
    assert "DRY_RUN (planning only)" in result.output
    assert "Importable (new):" in result.output
    assert not (raw_dir / "market_2016-07-26.csv").exists()
    assert repo.get_date_state("2016-07-26") is None


def test_apply_mode_imports_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, raw_dir, source_dir = setup_env(tmp_path, monkeypatch)
    source_csv = source_dir / "market_2016-07-26.csv"
    create_sample_csv(source_csv)

    result = runner.invoke(
        app, ["import-csv", "--source", str(source_dir), "--apply"]
    )

    assert result.exit_code == 0
    assert "APPLY (actual import)" in result.output
    assert "Imported (new):" in result.output
    assert (raw_dir / "market_2016-07-26.csv").exists()

    state = repo.get_date_state("2016-07-26")
    assert state is not None
    assert state.status.value == "VERIFIED_TRADING_DATA"


def test_recursive_flag_discovers_nested_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, raw_dir, source_dir = setup_env(tmp_path, monkeypatch)
    create_sample_csv(source_dir / "market_2016-07-26.csv")
    create_sample_csv(source_dir / "nested" / "market_2016-07-27.csv")

    result = runner.invoke(
        app, ["import-csv", "--source", str(source_dir), "--recursive", "--apply"]
    )

    assert result.exit_code == 0
    assert "Discovered files:" in result.output
    assert (raw_dir / "market_2016-07-26.csv").exists()
    assert (raw_dir / "market_2016-07-27.csv").exists()


def test_json_output_parses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, _, source_dir = setup_env(tmp_path, monkeypatch)
    create_sample_csv(source_dir / "market_2016-07-26.csv")

    result = runner.invoke(
        app, ["import-csv", "--source", str(source_dir), "--json"]
    )

    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["mode"] == "DRY_RUN"
    assert data["discovered_count"] == 1
    assert data["importable_count"] == 1
    assert len(data["results"]) == 1
    assert data["results"][0]["action"] == "IMPORT"


def test_conflict_presentation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, raw_dir, source_dir = setup_env(tmp_path, monkeypatch)

    source_csv = source_dir / "market_2016-07-26.csv"
    dest_csv = raw_dir / "market_2016-07-26.csv"

    create_sample_csv(source_csv, rows=(_row("AAA", 1),))
    create_sample_csv(dest_csv, rows=(_row("AAA", 1), _row("BBB", 2)))

    result = runner.invoke(
        app, ["import-csv", "--source", str(source_dir), "--apply"]
    )

    assert result.exit_code == 1
    assert "Conflicts:" in result.output
    assert "CONFLICT" in result.output


def test_second_apply_reports_already_present_no_op(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, _, source_dir = setup_env(tmp_path, monkeypatch)
    create_sample_csv(source_dir / "market_2016-07-26.csv")

    res1 = runner.invoke(
        app, ["import-csv", "--source", str(source_dir), "--apply"]
    )
    assert res1.exit_code == 0

    res2 = runner.invoke(
        app, ["import-csv", "--source", str(source_dir), "--apply"]
    )
    assert res2.exit_code == 0
    assert "Already present:" in res2.output


def test_source_files_remain_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, _, source_dir = setup_env(tmp_path, monkeypatch)
    source_csv = source_dir / "market_2016-07-26.csv"
    content_before = create_sample_csv(source_csv)

    runner.invoke(app, ["import-csv", "--source", str(source_dir), "--apply"])

    assert source_csv.read_bytes() == content_before
