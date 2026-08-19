from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path

import psx_data_sync.exporter as exporter
import pytest
from psx_data_sync.exporter import (
    canonical_csv_bytes,
    inspect_existing_canonical_file,
    save_canonical_csv,
)
from psx_data_sync.parser import parse_equity_rows
from psx_data_sync.state import SaveStatus
from psx_data_sync.validator import validate_rows


REQUESTED_DATE = date(2026, 8, 5)


def valid_rows(fixture_bytes):
    return validate_rows(
        parse_equity_rows(fixture_bytes("valid_market.html"))
    ).valid_rows


def test_canonical_content_is_sorted_and_checksum_is_deterministic(
    fixture_bytes,
) -> None:
    rows = valid_rows(fixture_bytes)
    first = canonical_csv_bytes(rows)
    second = canonical_csv_bytes(reversed(rows))

    assert first == second
    assert first.decode().splitlines()[1].startswith("AAA,")
    assert hashlib.sha256(first).hexdigest() == hashlib.sha256(second).hexdigest()


def test_atomic_save_renames_temporary_file(
    tmp_path: Path, fixture_bytes, monkeypatch
) -> None:
    calls: list[tuple[Path, Path]] = []
    real_replace = exporter.os.replace

    def observed_replace(source, target) -> None:
        calls.append((Path(source), Path(target)))
        real_replace(source, target)

    monkeypatch.setattr(exporter.os, "replace", observed_replace)

    result = save_canonical_csv(valid_rows(fixture_bytes), REQUESTED_DATE, tmp_path)

    assert result.status is SaveStatus.CREATED
    assert result.path.exists()
    assert calls and calls[0][0].suffix == ".tmp"
    assert calls[0][1] == result.path
    assert not list(tmp_path.glob("*.tmp"))


def test_identical_existing_file_is_unchanged(tmp_path: Path, fixture_bytes) -> None:
    rows = valid_rows(fixture_bytes)
    created = save_canonical_csv(rows, REQUESTED_DATE, tmp_path)
    before = created.path.read_bytes()

    repeated = save_canonical_csv(reversed(rows), REQUESTED_DATE, tmp_path)

    assert repeated.status is SaveStatus.ALREADY_PRESENT
    assert repeated.checksum == created.checksum
    assert repeated.path.read_bytes() == before


def test_valid_conflicting_file_is_not_overwritten(tmp_path: Path, fixture_bytes) -> None:
    rows = valid_rows(fixture_bytes)
    created = save_canonical_csv(rows[:1], REQUESTED_DATE, tmp_path)
    before = created.path.read_bytes()

    conflicting = save_canonical_csv(rows, REQUESTED_DATE, tmp_path)

    assert conflicting.status is SaveStatus.CONFLICT
    assert conflicting.path.read_bytes() == before


def test_invalid_existing_file_is_not_overwritten(tmp_path: Path, fixture_bytes) -> None:
    path = tmp_path / "market_2026-08-05.csv"
    path.write_text("corrupt\n", encoding="utf-8")
    before = path.read_bytes()

    result = save_canonical_csv(valid_rows(fixture_bytes), REQUESTED_DATE, tmp_path)

    assert result.status is SaveStatus.EXISTING_FILE_INVALID
    assert path.read_bytes() == before


def test_empty_data_is_never_saved(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="without valid rows"):
        save_canonical_csv([], REQUESTED_DATE, tmp_path)

    assert not list(tmp_path.glob("*.csv"))


def test_existing_canonical_file_inspection(tmp_path: Path, fixture_bytes) -> None:
    created = save_canonical_csv(
        valid_rows(fixture_bytes), REQUESTED_DATE, tmp_path
    )

    inspection = inspect_existing_canonical_file(REQUESTED_DATE, tmp_path)

    assert inspection.exists and inspection.valid
    assert inspection.row_count == 3
    assert inspection.checksum == created.checksum


def test_noncanonical_valid_csv_cannot_be_locally_skipped(
    tmp_path: Path, fixture_bytes
) -> None:
    created = save_canonical_csv(
        valid_rows(fixture_bytes), REQUESTED_DATE, tmp_path
    )
    content = created.path.read_text(encoding="utf-8")
    created.path.write_text(content.replace("AAA,100,", "AAA,100.0,"), encoding="utf-8")

    inspection = inspect_existing_canonical_file(REQUESTED_DATE, tmp_path)

    assert inspection.exists
    assert not inspection.valid
    assert "not canonical" in (inspection.error or "")
