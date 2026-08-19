from pathlib import Path

import pytest

from psx_data_sync.config import (
    CANONICAL_COLUMNS,
    DEFAULT_RANGE_WORKERS,
    MAX_RANGE_WORKERS,
    Settings,
)


def test_defaults_resolve_correctly() -> None:
    settings = Settings.from_env({})

    assert settings.historical_url == "https://dps.psx.com.pk/historical"
    assert settings.retry_attempts == 3
    assert settings.raw_output_dir == Path("data/raw")
    assert settings.state_db_path == Path("data/state/psx_sync.db")
    assert settings.canonical_columns == CANONICAL_COLUMNS
    assert settings.range_workers == DEFAULT_RANGE_WORKERS == 4
    assert settings.max_range_workers == MAX_RANGE_WORKERS == 16


def test_environment_overrides_are_typed(tmp_path: Path) -> None:
    settings = Settings.from_env(
        {
            "PSX_RETRY_ATTEMPTS": "5",
            "PSX_REQUEST_TIMEOUT_SECONDS": "12.5",
            "PSX_RAW_OUTPUT_DIR": str(tmp_path),
            "PSX_STATE_DB_PATH": str(tmp_path / "sync.db"),
            "PSX_RANGE_WORKERS": "2",
        }
    )

    assert settings.retry_attempts == 5
    assert settings.request_timeout_seconds == 12.5
    assert settings.raw_output_dir == tmp_path
    assert settings.state_db_path == tmp_path / "sync.db"
    assert settings.range_workers == 2


def test_invalid_environment_override_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        Settings.from_env({"PSX_RETRY_ATTEMPTS": "0"})

    with pytest.raises(ValueError, match="cannot exceed 16"):
        Settings.from_env({"PSX_MAX_RANGE_WORKERS": "500"})


def test_lower_worker_cap_also_lowers_implicit_default() -> None:
    settings = Settings.from_env({"PSX_MAX_RANGE_WORKERS": "2"})

    assert settings.max_range_workers == 2
    assert settings.range_workers == 2
