"""Typed configuration for the PSX single-date and range downloaders."""

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
MIN_RANGE_WORKERS = 1
DEFAULT_RANGE_WORKERS = 4
MAX_RANGE_WORKERS = 16
MAX_RECHECKS_PER_DATE_PER_RUN = 5


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
    range_workers: int = DEFAULT_RANGE_WORKERS
    max_range_workers: int = MAX_RANGE_WORKERS
    large_range_warning_days: int = 365
    user_agent: str = "psx-data-sync/0.4 (+https://dps.psx.com.pk/)"
    raw_output_dir: Path = Path("data/raw")
    state_db_path: Path = Path("data/state/psx_sync.db")
    repair_staging_dir: Path = Path("data/state/repair_staging")
    max_rechecks_per_date_per_run: int = 1
    reconciliation_cooldown_seconds: float = 86_400.0
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

        max_range_workers = _positive_int(
            values, "PSX_MAX_RANGE_WORKERS", defaults.max_range_workers
        )
        if max_range_workers > MAX_RANGE_WORKERS:
            raise ValueError(
                f"PSX_MAX_RANGE_WORKERS cannot exceed {MAX_RANGE_WORKERS}"
            )
        range_workers = _positive_int(
            values,
            "PSX_RANGE_WORKERS",
            min(defaults.range_workers, max_range_workers),
        )
        if range_workers > max_range_workers:
            raise ValueError(
                "PSX_RANGE_WORKERS cannot exceed PSX_MAX_RANGE_WORKERS"
            )
        max_rechecks = _positive_int(
            values,
            "PSX_MAX_RECHECKS_PER_DATE_PER_RUN",
            defaults.max_rechecks_per_date_per_run,
        )
        if max_rechecks > MAX_RECHECKS_PER_DATE_PER_RUN:
            raise ValueError(
                "PSX_MAX_RECHECKS_PER_DATE_PER_RUN cannot exceed "
                f"{MAX_RECHECKS_PER_DATE_PER_RUN}"
            )

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
            range_workers=range_workers,
            max_range_workers=max_range_workers,
            large_range_warning_days=_positive_int(
                values,
                "PSX_LARGE_RANGE_WARNING_DAYS",
                defaults.large_range_warning_days,
            ),
            user_agent=values.get("PSX_USER_AGENT", defaults.user_agent),
            raw_output_dir=Path(
                values.get("PSX_RAW_OUTPUT_DIR", str(defaults.raw_output_dir))
            ).expanduser(),
            state_db_path=Path(
                values.get("PSX_STATE_DB_PATH", str(defaults.state_db_path))
            ).expanduser(),
            repair_staging_dir=Path(
                values.get(
                    "PSX_REPAIR_STAGING_DIR", str(defaults.repair_staging_dir)
                )
            ).expanduser(),
            max_rechecks_per_date_per_run=max_rechecks,
            reconciliation_cooldown_seconds=_non_negative_float(
                values,
                "PSX_RECONCILIATION_COOLDOWN_SECONDS",
                defaults.reconciliation_cooldown_seconds,
            ),
        )
