"""Typed configuration for the PSX single-date downloader."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


CANONICAL_COLUMNS: tuple[str, ...] = (
    "symbol",
    "ldcp",
    "open",
    "high",
    "low",
    "close",
    "change",
    "change_percent",
    "volume",
)


def _positive_float(values: Mapping[str, str], name: str, default: float) -> float:
    raw = values.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _non_negative_float(
    values: Mapping[str, str], name: str, default: float
) -> float:
    raw = values.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value < 0:
        raise ValueError(f"{name} cannot be negative")
    return value


def _positive_int(values: Mapping[str, str], name: str, default: int) -> int:
    raw = values.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings with production-safe defaults."""

    historical_url: str = "https://dps.psx.com.pk/historical"
    request_timeout_seconds: float = 30.0
    connect_timeout_seconds: float = 10.0
    retry_attempts: int = 3
    retry_backoff_initial_seconds: float = 1.0
    retry_backoff_max_seconds: float = 8.0
    retry_jitter_fraction: float = 0.25
    user_agent: str = "psx-data-sync/0.1 (+https://dps.psx.com.pk/)"
    raw_output_dir: Path = Path("data/raw")
    canonical_columns: tuple[str, ...] = CANONICAL_COLUMNS

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "Settings":
        """Build settings from environment variables without requiring a .env file."""

        values = os.environ if environ is None else environ
        defaults = cls()
        historical_url = values.get("PSX_HISTORICAL_URL", defaults.historical_url)
        if not historical_url.startswith(("https://", "http://")):
            raise ValueError("PSX_HISTORICAL_URL must be an HTTP(S) URL")

        jitter = _non_negative_float(
            values, "PSX_RETRY_JITTER_FRACTION", defaults.retry_jitter_fraction
        )
        if jitter > 1:
            raise ValueError("PSX_RETRY_JITTER_FRACTION cannot exceed 1")

        return cls(
            historical_url=historical_url,
            request_timeout_seconds=_positive_float(
                values, "PSX_REQUEST_TIMEOUT_SECONDS", defaults.request_timeout_seconds
            ),
            connect_timeout_seconds=_positive_float(
                values, "PSX_CONNECT_TIMEOUT_SECONDS", defaults.connect_timeout_seconds
            ),
            retry_attempts=_positive_int(
                values, "PSX_RETRY_ATTEMPTS", defaults.retry_attempts
            ),
            retry_backoff_initial_seconds=_non_negative_float(
                values,
                "PSX_RETRY_BACKOFF_INITIAL_SECONDS",
                defaults.retry_backoff_initial_seconds,
            ),
            retry_backoff_max_seconds=_non_negative_float(
                values,
                "PSX_RETRY_BACKOFF_MAX_SECONDS",
                defaults.retry_backoff_max_seconds,
            ),
            retry_jitter_fraction=jitter,
            user_agent=values.get("PSX_USER_AGENT", defaults.user_agent),
            raw_output_dir=Path(
                values.get("PSX_RAW_OUTPUT_DIR", str(defaults.raw_output_dir))
            ).expanduser(),
        )
