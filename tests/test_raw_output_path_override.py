from __future__ import annotations

import os
from pathlib import Path

import pytest

from psx_data_sync.config import Settings
from psx_data_sync.importer import import_local_csv_directory
from psx_data_sync.state_db import StateRepository


def test_import_apply_writes_to_configured_raw_output_dir(tmp_path: Path) -> None:
    """Verify Apply Import writes canonical CSV only to configured raw_output_dir."""
    custom_raw_dir = tmp_path / "custom_raw"
    db_path = tmp_path / "state" / "psx_sync.db"
    source_dir = tmp_path / "source_csvs"
    source_dir.mkdir(parents=True, exist_ok=True)

    # Create a valid canonical source CSV
    sample_csv = source_dir / "market_2026-08-01.csv"
    sample_csv.write_text(
        "symbol,ldcp,open,high,low,close,change,change_percent,volume\n"
        "OGDC,100,101,105,99,104,4,4,1000000\n",
        encoding="utf-8",
    )

    settings = Settings(
        raw_output_dir=custom_raw_dir,
        state_db_path=db_path,
    )
    repo = StateRepository(
        settings.state_db_path,
        project_root=tmp_path,
        raw_output_dir=settings.raw_output_dir,
    )
    repo.initialize()

    # Execute import in apply mode
    res = import_local_csv_directory(repo, source_dir, dry_run=False)
    assert res.imported_count == 1

    # Verify canonical CSV was written only to custom_raw_dir
    expected_output = custom_raw_dir / "market_2026-08-01.csv"
    assert expected_output.exists()
    assert expected_output.parent.resolve() == custom_raw_dir.resolve()

    # Verify default relative path data/raw/market_2026-08-01.csv does NOT exist
    default_dev_raw = Path("data/raw") / "market_2026-08-01.csv"
    assert not default_dev_raw.exists()


def test_psx_raw_output_dir_environment_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify PSX_RAW_OUTPUT_DIR environment variable is honored by Settings and StateRepository."""
    env_raw_dir = tmp_path / "env_override_raw"
    monkeypatch.setenv("PSX_RAW_OUTPUT_DIR", str(env_raw_dir))

    settings = Settings.from_env()
    assert settings.raw_output_dir.resolve() == env_raw_dir.resolve()

    repo = StateRepository(
        settings.state_db_path,
        raw_output_dir=settings.raw_output_dir,
    )
    assert repo.raw_output_dir.resolve() == env_raw_dir.resolve()
