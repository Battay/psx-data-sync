from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from pathlib import Path

from psx_data_sync.client import PSXClientError
from psx_data_sync.config import Settings
from psx_data_sync.downloader import SingleDateDownloader, validate_requested_date
from psx_data_sync.exporter import save_canonical_csv
from psx_data_sync.parser import parse_equity_rows
from psx_data_sync.state import (
    ClientFailureKind,
    DownloadStatus,
    FetchResponse,
)
from psx_data_sync.validator import validate_rows


class FakeClient:
    def __init__(self, outcomes: Iterable[FetchResponse | Exception]) -> None:
        self.outcomes = iter(outcomes)
        self.calls = 0

    def fetch(self, requested_date: date) -> FetchResponse:
        self.calls += 1
        outcome = next(self.outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def settings(tmp_path: Path, attempts: int = 3) -> Settings:
    return Settings(
        raw_output_dir=tmp_path,
        retry_attempts=attempts,
        retry_backoff_initial_seconds=0,
        retry_backoff_max_seconds=0,
    )


def response(content: bytes) -> FetchResponse:
    return FetchResponse(status_code=200, content=content)


def test_valid_date_is_saved(tmp_path: Path, fixture_bytes) -> None:
    client = FakeClient([response(fixture_bytes("valid_market.html"))])
    result = SingleDateDownloader(settings(tmp_path), client).download("2026-08-05")

    assert result.status is DownloadStatus.TRADING_DATA
    assert result.attempts == 1
    assert result.parsed_row_count == result.valid_row_count == 3
    assert result.rejected_row_count == 0
    assert result.saved_path == tmp_path / "market_2026-08-05.csv"
    assert result.saved_path.exists()
    assert result.checksum


def test_empty_content_retries_and_is_never_saved(tmp_path: Path, fixture_bytes) -> None:
    empty = response(fixture_bytes("empty_shell.html"))
    client = FakeClient([empty, empty, empty])

    result = SingleDateDownloader(settings(tmp_path), client).download("2026-08-05")

    assert result.status is DownloadStatus.NON_TRADING_OR_EMPTY
    assert result.attempts == 3
    assert not list(tmp_path.glob("*.csv"))
    assert "not proof of a non-trading day" in result.warnings[-1]


def test_temporary_empty_content_can_recover(tmp_path: Path, fixture_bytes) -> None:
    client = FakeClient(
        [
            response(fixture_bytes("empty_shell.html")),
            response(fixture_bytes("valid_market.html")),
        ]
    )

    result = SingleDateDownloader(settings(tmp_path), client).download("2026-08-05")

    assert result.status is DownloadStatus.TRADING_DATA
    assert result.attempts == 2
    assert "retried" in result.warnings[0]


def test_failure_never_overwrites_existing_valid_file(
    tmp_path: Path, fixture_bytes
) -> None:
    rows = validate_rows(
        parse_equity_rows(fixture_bytes("valid_market.html"))
    ).valid_rows
    existing = save_canonical_csv(rows, date(2026, 8, 5), tmp_path)
    before = existing.path.read_bytes()
    error = PSXClientError(
        "offline",
        kind=ClientFailureKind.CONNECTION,
        retryable=True,
    )
    client = FakeClient([error, error, error])

    result = SingleDateDownloader(settings(tmp_path), client).download("2026-08-05")

    assert result.status is DownloadStatus.TEMPORARY_FAILURE
    assert existing.path.read_bytes() == before


def test_identical_existing_file_is_reported(tmp_path: Path, fixture_bytes) -> None:
    content = fixture_bytes("valid_market.html")
    client = FakeClient([response(content), response(content)])
    downloader = SingleDateDownloader(settings(tmp_path), client)

    first = downloader.download("2026-08-05")
    second = downloader.download("2026-08-05")

    assert first.status is DownloadStatus.TRADING_DATA
    assert second.status is DownloadStatus.ALREADY_PRESENT
    assert second.checksum == first.checksum


def test_conflicting_existing_file_is_reported(tmp_path: Path, fixture_bytes) -> None:
    content = fixture_bytes("valid_market.html")
    rows = validate_rows(parse_equity_rows(content)).valid_rows
    existing = save_canonical_csv(rows[:1], date(2026, 8, 5), tmp_path)
    before = existing.path.read_bytes()

    result = SingleDateDownloader(
        settings(tmp_path), FakeClient([response(content)])
    ).download("2026-08-05")

    assert result.status is DownloadStatus.FILE_CONFLICT
    assert existing.path.read_bytes() == before


def test_invalid_existing_file_is_reported(tmp_path: Path, fixture_bytes) -> None:
    path = tmp_path / "market_2026-08-05.csv"
    path.write_text("broken", encoding="utf-8")

    result = SingleDateDownloader(
        settings(tmp_path), FakeClient([response(fixture_bytes("valid_market.html"))])
    ).download("2026-08-05")

    assert result.status is DownloadStatus.EXISTING_FILE_INVALID
    assert path.read_text(encoding="utf-8") == "broken"


def test_all_invalid_rows_fail_without_writing(tmp_path: Path, fixture_bytes) -> None:
    result = SingleDateDownloader(
        settings(tmp_path),
        FakeClient([response(fixture_bytes("malformed_numeric.html"))]),
    ).download("2026-08-05")

    assert result.status is DownloadStatus.VALIDATION_FAILURE
    assert result.rejected_row_count == 1
    assert not list(tmp_path.glob("*.csv"))


def test_bad_row_is_rejected_without_spoiling_valid_date(
    tmp_path: Path, fixture_bytes
) -> None:
    mixed = fixture_bytes("valid_market.html") + fixture_bytes("null_row.html")
    result = SingleDateDownloader(
        settings(tmp_path), FakeClient([response(mixed)])
    ).download("2026-08-05")

    assert result.status is DownloadStatus.TRADING_DATA
    assert result.parsed_row_count == 4
    assert result.valid_row_count == 3
    assert result.rejected_row_count == 1
    assert result.saved_path and result.saved_path.exists()


def test_strict_date_validation_happens_before_fetch(tmp_path: Path) -> None:
    client = FakeClient([])
    result = SingleDateDownloader(settings(tmp_path), client).download("2026-8-5")

    assert result.status is DownloadStatus.INVALID_DATE
    assert client.calls == 0


def test_future_date_is_rejected() -> None:
    try:
        validate_requested_date("2999-01-01", today=date(2026, 8, 19))
    except ValueError as exc:
        assert "future" in str(exc)
    else:
        raise AssertionError("future date was accepted")


def test_non_retryable_http_error_stops_immediately(tmp_path: Path) -> None:
    error = PSXClientError(
        "HTTP 404",
        kind=ClientFailureKind.HTTP,
        retryable=False,
        http_status=404,
    )
    client = FakeClient([error])

    result = SingleDateDownloader(settings(tmp_path), client).download("2026-08-05")

    assert result.status is DownloadStatus.HTTP_FAILURE
    assert result.attempts == 1
    assert result.http_status == 404
