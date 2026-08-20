from __future__ import annotations

from datetime import date
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from psx_data_sync.exporter import canonical_csv_bytes
from psx_data_sync.parquet_store import (
    PARQUET_COLUMNS,
    PARQUET_SCHEMA_VERSION,
    inspect_parquet_file,
    parquet_partition_path,
    write_parquet_partition,
)
from psx_data_sync.state import ValidEquityRow


def _row(symbol: str, row_index: int) -> ValidEquityRow:
    return ValidEquityRow(
        row_index=row_index,
        symbol=symbol,
        ldcp=Decimal("100.10"),
        open=Decimal("101.20"),
        high=Decimal("105.30"),
        low=Decimal("99.40"),
        close=Decimal("104.50"),
        change=Decimal("4.40"),
        change_percent=Decimal("4.3956"),
        volume=123456,
    )


def _write_source(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_csv_bytes(rows))


def test_partition_path_is_date_partitioned(tmp_path):
    target = parquet_partition_path(
        tmp_path,
        date(2026, 8, 7),
    )

    assert target == (
        tmp_path
        / "market"
        / "market_date=2026-08-07"
        / "part-0.parquet"
    )


def test_verified_canonical_csv_exports_to_valid_parquet(tmp_path):
    source = tmp_path / "market_2026-08-07.csv"
    output = tmp_path / "parquet"

    _write_source(
        source,
        (
            _row("ZZZ", 1),
            _row("AAA", 2),
        ),
    )

    result = write_parquet_partition(
        date(2026, 8, 7),
        source,
        output,
    )

    assert result.row_count == 2
    assert result.path.exists()

    inspection = inspect_parquet_file(
        result.path,
        expected_market_date=date(2026, 8, 7),
        expected_source_checksum=result.source_csv_checksum,
        expected_source_row_count=2,
    )

    assert inspection.valid is True
    assert inspection.schema_version == PARQUET_SCHEMA_VERSION
    assert inspection.row_count == 2


def test_parquet_schema_and_row_order_are_deterministic(tmp_path):
    source = tmp_path / "market_2026-08-07.csv"
    output = tmp_path / "parquet"

    _write_source(
        source,
        (
            _row("ZZZ", 1),
            _row("AAA", 2),
            _row("MEBL", 3),
        ),
    )

    result = write_parquet_partition(
        date(2026, 8, 7),
        source,
        output,
    )

    table = pq.read_table(result.path)

    assert tuple(table.column_names) == PARQUET_COLUMNS
    assert table.column("symbol").to_pylist() == [
        "AAA",
        "MEBL",
        "ZZZ",
    ]

    assert table.column("market_date").to_pylist() == [
        date(2026, 8, 7),
        date(2026, 8, 7),
        date(2026, 8, 7),
    ]


def test_source_csv_is_never_modified(tmp_path):
    source = tmp_path / "market_2026-08-07.csv"
    output = tmp_path / "parquet"

    _write_source(source, (_row("AAA", 1),))
    before = source.read_bytes()

    write_parquet_partition(
        date(2026, 8, 7),
        source,
        output,
    )

    assert source.read_bytes() == before


def test_invalid_source_csv_is_rejected(tmp_path):
    source = tmp_path / "market_2026-08-07.csv"
    output = tmp_path / "parquet"

    source.write_text(
        "symbol,ldcp,open,high,low,close,change,change_percent,volume\n"
        "BAD,null,1,2,1,2,1,1,100\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        write_parquet_partition(
            date(2026, 8, 7),
            source,
            output,
        )

    assert not parquet_partition_path(
        output,
        date(2026, 8, 7),
    ).exists()


def test_corrupt_parquet_is_detected(tmp_path):
    path = parquet_partition_path(
        tmp_path,
        date(2026, 8, 7),
    )

    path.parent.mkdir(parents=True)
    path.write_bytes(b"not parquet data")

    inspection = inspect_parquet_file(
        path,
        expected_market_date=date(2026, 8, 7),
    )

    assert inspection.exists is True
    assert inspection.valid is False


def test_wrong_expected_source_checksum_is_stale(tmp_path):
    source = tmp_path / "market_2026-08-07.csv"
    output = tmp_path / "parquet"

    _write_source(source, (_row("AAA", 1),))

    result = write_parquet_partition(
        date(2026, 8, 7),
        source,
        output,
    )

    inspection = inspect_parquet_file(
        result.path,
        expected_market_date=date(2026, 8, 7),
        expected_source_checksum="0" * 64,
        expected_source_row_count=1,
    )

    assert inspection.valid is False
    assert inspection.error == "Parquet source checksum is stale"


def test_repeated_build_has_same_logical_contents(tmp_path):
    source = tmp_path / "market_2026-08-07.csv"

    _write_source(
        source,
        (
            _row("BBB", 1),
            _row("AAA", 2),
        ),
    )

    first = write_parquet_partition(
        date(2026, 8, 7),
        source,
        tmp_path / "first",
    )

    second = write_parquet_partition(
        date(2026, 8, 7),
        source,
        tmp_path / "second",
    )

    first_table = pq.read_table(first.path)
    second_table = pq.read_table(second.path)

    assert first_table.equals(second_table)
    assert first.source_csv_checksum == second.source_csv_checksum
