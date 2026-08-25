from __future__ import annotations

from datetime import date
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from psx_data_sync.exporter import canonical_csv_bytes
from psx_data_sync.parquet_store import (
    PARQUET_COLUMNS,
    PARQUET_SCHEMA_VERSION,
    ParquetSource,
    compute_source_identity,
    consolidated_parquet_path,
    inspect_parquet_file,
    write_consolidated_parquet,
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


def _source(tmp_path, day: date, symbols: tuple[str, ...]) -> ParquetSource:
    rows = tuple(_row(symbol, index) for index, symbol in enumerate(symbols, 1))
    path = tmp_path / "raw" / f"market_{day.isoformat()}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    content = canonical_csv_bytes(rows)
    path.write_bytes(content)
    import hashlib

    return ParquetSource(day, path, hashlib.sha256(content).hexdigest(), len(rows))


def test_consolidated_path_is_one_file(tmp_path):
    assert consolidated_parquet_path(tmp_path) == tmp_path / "market.parquet"


def test_multiple_dates_build_one_zstd_file_in_deterministic_order(tmp_path):
    later = _source(tmp_path, date(2026, 8, 8), ("ZZZ", "AAA"))
    earlier = _source(tmp_path, date(2026, 8, 7), ("MEBL", "AAA"))

    result = write_consolidated_parquet((later, earlier), tmp_path / "parquet")

    assert result.path == tmp_path / "parquet" / "market.parquet"
    assert list((tmp_path / "parquet").rglob("*.parquet")) == [result.path]
    table = pq.read_table(result.path)
    assert tuple(table.column_names) == PARQUET_COLUMNS
    assert list(zip(table["market_date"].to_pylist(), table["symbol"].to_pylist())) == [
        (date(2026, 8, 7), "AAA"),
        (date(2026, 8, 7), "MEBL"),
        (date(2026, 8, 8), "AAA"),
        (date(2026, 8, 8), "ZZZ"),
    ]
    metadata = pq.ParquetFile(result.path).metadata
    assert metadata.row_group(0).column(0).compression == "ZSTD"
    inspection = inspect_parquet_file(result.path)
    assert inspection.valid
    assert inspection.schema_version == PARQUET_SCHEMA_VERSION
    assert inspection.source_date_count == 2
    assert inspection.source_row_count == 4


def test_source_identity_is_order_independent_and_content_sensitive(tmp_path):
    first = _source(tmp_path, date(2026, 8, 7), ("AAA",))
    second = _source(tmp_path, date(2026, 8, 8), ("BBB",))
    assert compute_source_identity((first, second)) == compute_source_identity((second, first))

    changed = ParquetSource(
        second.market_date, second.path, "f" * 64, second.row_count
    )
    assert compute_source_identity((first, second)) != compute_source_identity((first, changed))
    assert compute_source_identity((first, second)) != compute_source_identity((first,))


def test_duplicate_market_date_symbol_is_rejected(tmp_path):
    source = _source(tmp_path, date(2026, 8, 7), ("AAA", "AAA"))
    with pytest.raises(ValueError, match="duplicate symbol"):
        write_consolidated_parquet((source,), tmp_path / "parquet")
    assert not consolidated_parquet_path(tmp_path / "parquet").exists()


def test_source_csv_is_unchanged(tmp_path):
    source = _source(tmp_path, date(2026, 8, 7), ("AAA", "BBB"))
    before = source.path.read_bytes()
    write_consolidated_parquet((source,), tmp_path / "parquet")
    assert source.path.read_bytes() == before


def test_corrupt_parquet_is_detected(tmp_path):
    path = consolidated_parquet_path(tmp_path)
    path.write_bytes(b"not parquet")
    inspection = inspect_parquet_file(path)
    assert inspection.exists
    assert not inspection.valid
    assert "cannot be read" in (inspection.error or "")


def test_missing_required_metadata_is_corrupt(tmp_path):
    source = _source(tmp_path, date(2026, 8, 7), ("AAA",))
    result = write_consolidated_parquet((source,), tmp_path / "parquet")
    table = pq.read_table(result.path).replace_schema_metadata(None)
    pq.write_table(table, result.path, compression="zstd")

    inspection = inspect_parquet_file(result.path)
    assert not inspection.valid
    assert "metadata" in (inspection.error or "")


def test_repeated_build_is_byte_deterministic(tmp_path):
    sources = (
        _source(tmp_path, date(2026, 8, 8), ("ZZZ", "AAA")),
        _source(tmp_path, date(2026, 8, 7), ("BBB",)),
    )
    first = write_consolidated_parquet(sources, tmp_path / "first")
    second = write_consolidated_parquet(reversed(sources), tmp_path / "second")

    assert first.checksum == second.checksum
    assert first.path.read_bytes() == second.path.read_bytes()


def test_atomic_failure_preserves_previous_final_and_cleans_temp(tmp_path, monkeypatch):
    source = _source(tmp_path, date(2026, 8, 7), ("AAA",))
    output = tmp_path / "parquet"
    first = write_consolidated_parquet((source,), output)
    before = first.path.read_bytes()

    from psx_data_sync import parquet_store

    def fail_write(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(parquet_store, "_write_parquet_file", fail_write)
    with pytest.raises(OSError, match="disk full"):
        write_consolidated_parquet((source,), output)

    assert first.path.read_bytes() == before
    assert not list(output.glob(".market.parquet.*.tmp"))


def test_atomic_replace_failure_preserves_previous_final(tmp_path, monkeypatch):
    source = _source(tmp_path, date(2026, 8, 7), ("AAA",))
    output = tmp_path / "parquet"
    first = write_consolidated_parquet((source,), output)
    before = first.path.read_bytes()

    from psx_data_sync import parquet_store

    def fail_replace(*args, **kwargs):
        raise OSError("replace failed")

    monkeypatch.setattr(parquet_store.os, "replace", fail_replace)
    with pytest.raises(OSError, match="replace failed"):
        write_consolidated_parquet((source,), output)

    assert first.path.read_bytes() == before
    assert not list(output.glob(".market.parquet.*.tmp"))
