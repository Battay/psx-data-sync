from pathlib import Path

import pytest

from psx_data_sync.config import CANONICAL_COLUMNS, Settings


def test_defaults_resolve_correctly() -> None:
    settings = Settings.from_env({})

    assert settings.historical_url == "https://dps.psx.com.pk/historical"
    assert settings.retry_attempts == 3
    assert settings.raw_output_dir == Path("data/raw")
    assert settings.canonical_columns == CANONICAL_COLUMNS


def test_environment_overrides_are_typed(tmp_path: Path) -> None:
    settings = Settings.from_env(
        {
            "PSX_RETRY_ATTEMPTS": "5",
            "PSX_REQUEST_TIMEOUT_SECONDS": "12.5",
            "PSX_RAW_OUTPUT_DIR": str(tmp_path),
        }
    )

    assert settings.retry_attempts == 5
    assert settings.request_timeout_seconds == 12.5
    assert settings.raw_output_dir == tmp_path


def test_invalid_environment_override_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        Settings.from_env({"PSX_RETRY_ATTEMPTS": "0"})
