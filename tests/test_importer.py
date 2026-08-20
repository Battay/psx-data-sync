from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from psx_data_sync.exporter import CANONICAL_COLUMNS, canonical_csv_bytes, sha256_bytes
from psx_data_sync.importer import (
    LocalImportAction,
    import_local_csv_directory,
    import_local_csv_file,
)
from psx_data_sync.state import (
    PersistentSyncStatus,
    ValidEquityRow,
)
from psx_data_sync.state_db import StateRepository


def make_repository(database_path: Path, project_root: Path) -> StateRepository:
    repo = StateRepository(
        database_path,
        project_root=project_root,
        source_endpoint="https://dps.psx.com.pk/historical",
    )
    repo.initialize()
    return repo


def _row(symbol: str, row_index: int = 1) -> ValidEquityRow:
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


def create_sample_csv(
    path: Path, rows: tuple[ValidEquityRow, ...] = (_row("AAA", 1), _row("BBB", 2))
) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = canonical_csv_bytes(rows)
    path.write_bytes(content)
    return content


def create_legacy_csv(
    path: Path,
    date_text: str = "2026-08-05",
    lines: tuple[str, ...] = (
        "786,2026-08-05,25.97,28.57,28.57,28.57,28.57,2.6000000000000014,10.011551790527538,416457",
        "AABS,2026-08-05,875.42,876.21,891.49,851.0,876.29,0.8700000000000045,0.09938086861163836,316",
    ),
) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = "symbol,date,ldcp,open,high,low,close,change,change_percent,volume\n"
    content = (header + "\n".join(lines) + "\n").encode("utf-8")
    path.write_bytes(content)
    return content


def test_valid_new_source_import_dry_run_and_apply(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_dir = tmp_path / "virtual_trader_archive"
    source_csv = source_dir / "market_2016-07-26.csv"
    content = create_sample_csv(source_csv)

    dest_dir = tmp_path / "data" / "raw"
    dest_csv = dest_dir / "market_2016-07-26.csv"

    # Dry run
    dry_res = import_local_csv_file(
        repo, source_csv, destination_dir=dest_dir, dry_run=True
    )
    assert dry_res.action is LocalImportAction.IMPORT
    assert dry_res.imported is False
    assert not dest_csv.exists()
    assert repo.get_date_state("2016-07-26") is None

    # Apply mode
    apply_res = import_local_csv_file(
        repo, source_csv, destination_dir=dest_dir, dry_run=False
    )
    assert apply_res.action is LocalImportAction.IMPORT
    assert apply_res.imported is True
    assert dest_csv.exists()
    assert dest_csv.read_bytes() == content
    assert source_csv.read_bytes() == content

    state = repo.get_date_state("2016-07-26")
    assert state is not None
    assert state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert state.evidence_state == "LOCAL_CSV_SHA256_VERIFIED"
    assert state.valid_row_count == 2
    assert state.csv_checksum_sha256 == apply_res.source_checksum


def test_valid_legacy_file_converts_successfully(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_csv = tmp_path / "source" / "market_2026-08-05.csv"
    legacy_content = create_legacy_csv(source_csv, "2026-08-05")

    dest_dir = tmp_path / "data" / "raw"
    dest_csv = dest_dir / "market_2026-08-05.csv"

    dry_res = import_local_csv_file(
        repo, source_csv, destination_dir=dest_dir, dry_run=True
    )
    assert dry_res.action is LocalImportAction.IMPORT
    assert dry_res.imported is False
    assert not dest_csv.exists()
    assert repo.get_date_state("2026-08-05") is None

    apply_res = import_local_csv_file(
        repo, source_csv, destination_dir=dest_dir, dry_run=False
    )
    assert apply_res.action is LocalImportAction.IMPORT
    assert apply_res.imported is True
    assert dest_csv.exists()

    assert source_csv.read_bytes() == legacy_content

    dest_lines = dest_csv.read_text("utf-8").splitlines()
    assert dest_lines[0] == ",".join(CANONICAL_COLUMNS)
    assert "date" not in dest_lines[0]
    assert len(dest_lines) == 3
    assert "786,25.97,28.57,28.57,28.57,28.57,2.6,10.01,416457" in dest_lines[1]

    state = repo.get_date_state("2026-08-05")
    assert state is not None
    assert state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert state.valid_row_count == 2
    assert state.csv_checksum_sha256 == apply_res.source_checksum

    second_res = import_local_csv_file(
        repo, source_csv, destination_dir=dest_dir, dry_run=False
    )
    assert second_res.action is LocalImportAction.ALREADY_PRESENT
    assert second_res.imported is False


def test_scientific_notation_accepted(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_csv = tmp_path / "source" / "market_2026-08-05.csv"
    create_legacy_csv(
        source_csv,
        "2026-08-05",
        lines=(
            "AABS,2026-08-05,875.42,876.21,891.49,851.0,876.29,9.999999997489795e-05,-8.695652174102851e-05,316",
        ),
    )

    dest_dir = tmp_path / "data" / "raw"
    res = import_local_csv_file(repo, source_csv, destination_dir=dest_dir, dry_run=False)
    assert res.action is LocalImportAction.IMPORT
    assert res.valid is True
    assert res.row_count == 1
    assert res.rejected_row_count == 0


def test_nan_inf_malformed_rejected(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_csv = tmp_path / "source" / "market_2026-08-05.csv"
    create_legacy_csv(
        source_csv,
        "2026-08-05",
        lines=(
            "AAA,2026-08-05,NaN,10.0,10.0,10.0,10.0,0.0,0.0,100",
            "BBB,2026-08-05,10.0,inf,10.0,10.0,10.0,0.0,0.0,100",
            "CCC,2026-08-05,10.0,10.0,10.0,10.0,10.0,malformed,0.0,100",
        ),
    )

    res = import_local_csv_file(repo, source_csv, dry_run=False)
    assert res.action is LocalImportAction.INVALID_SOURCE
    assert res.valid is False


def test_legacy_row_level_rejection_keeps_valid_rows(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_csv = tmp_path / "source" / "market_2026-08-05.csv"
    create_legacy_csv(
        source_csv,
        "2026-08-05",
        lines=(
            "AAA,2026-08-05,10.0,10.0,10.0,10.0,10.0,0.0,0.0,100",  # Valid
            "BBB,2026-08-05,10.0,10.0,10.0,10.0,-5.0,0.0,0.0,100",  # Invalid close (-5.0 <= 0)
            "CCC,2026-08-05,10.0,10.0,10.0,10.0,10.0,0.0,0.0,100",  # Valid
        ),
    )

    dest_dir = tmp_path / "data" / "raw"
    dest_csv = dest_dir / "market_2026-08-05.csv"

    res = import_local_csv_file(repo, source_csv, destination_dir=dest_dir, dry_run=False)
    assert res.action is LocalImportAction.IMPORT
    assert res.valid is True
    assert res.row_count == 2
    assert res.rejected_row_count == 1

    dest_text = dest_csv.read_text("utf-8")
    assert "AAA" in dest_text
    assert "CCC" in dest_text
    assert "BBB" not in dest_text  # Invalid row absent from canonical output


def test_zero_valid_rows_rejects_file(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_csv = tmp_path / "source" / "market_2026-08-05.csv"
    create_legacy_csv(
        source_csv,
        "2026-08-05",
        lines=(
            "AAA,2026-08-05,10.0,10.0,10.0,10.0,-5.0,0.0,0.0,100",  # Invalid close
        ),
    )

    res = import_local_csv_file(repo, source_csv, dry_run=False)
    assert res.action is LocalImportAction.INVALID_SOURCE
    assert res.valid is False


def test_legacy_row_date_mismatch_whole_file_rejection(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_csv = tmp_path / "source" / "market_2026-08-05.csv"
    create_legacy_csv(
        source_csv,
        "2026-08-05",
        lines=(
            "AAA,2026-08-05,10.0,10.0,10.0,10.0,10.0,0.0,0.0,100",
            "BBB,2026-08-04,10.0,10.0,10.0,10.0,10.0,0.0,0.0,100",  # Date mismatch!
        ),
    )

    res = import_local_csv_file(repo, source_csv, dry_run=False)
    assert res.action is LocalImportAction.INVALID_SOURCE
    assert res.valid is False
    assert "does not match market date" in (res.error or "")


def test_legacy_canonical_identity_matches_existing_destination_already_present(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_dir = tmp_path / "source"
    dest_dir = tmp_path / "data" / "raw"

    # Seed 9-column canonical destination file and state
    row_aaa = ValidEquityRow(
        row_index=1,
        symbol="AAA",
        ldcp=Decimal("100.10"),
        open=Decimal("101.20"),
        high=Decimal("105.30"),
        low=Decimal("99.40"),
        close=Decimal("104.50"),
        change=Decimal("4.40"),
        change_percent=Decimal("4.40"),
        volume=123456,
    )
    row_bbb = ValidEquityRow(
        row_index=2,
        symbol="BBB",
        ldcp=Decimal("100.10"),
        open=Decimal("101.20"),
        high=Decimal("105.30"),
        low=Decimal("99.40"),
        close=Decimal("104.50"),
        change=Decimal("4.40"),
        change_percent=Decimal("4.40"),
        volume=123456,
    )
    dest_csv = dest_dir / "market_2026-08-05.csv"
    canonical_bytes_expected = create_sample_csv(dest_csv, rows=(row_aaa, row_bbb))
    expected_checksum = sha256_bytes(canonical_bytes_expected)
    repo.index_local_file(dest_csv)

    # Create 10-column legacy source file whose canonicalized bytes match exactly
    source_csv = source_dir / "market_2026-08-05.csv"
    create_legacy_csv(
        source_csv,
        "2026-08-05",
        lines=(
            "AAA,2026-08-05,100.1,101.2,105.3,99.4,104.5,4.4,4.3956,123456",
            "BBB,2026-08-05,100.1,101.2,105.3,99.4,104.5,4.4,4.3956,123456",
        ),
    )
    source_raw_before = source_csv.read_bytes()
    dest_raw_before = dest_csv.read_bytes()

    # Import attempt must return ALREADY_PRESENT without destination rewrite or DB mutation
    result = import_local_csv_file(repo, source_csv, destination_dir=dest_dir, dry_run=False)
    assert result.action is LocalImportAction.ALREADY_PRESENT
    assert result.imported is False
    assert result.source_checksum == expected_checksum
    assert dest_csv.read_bytes() == dest_raw_before
    assert source_csv.read_bytes() == source_raw_before


def test_genuine_canonical_difference_returns_conflict(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_dir = tmp_path / "source"
    dest_dir = tmp_path / "data" / "raw"

    dest_csv = dest_dir / "market_2026-08-05.csv"
    create_sample_csv(dest_csv, rows=(_row("AAA", 1),))
    repo.index_local_file(dest_csv)

    source_csv = source_dir / "market_2026-08-05.csv"
    create_legacy_csv(
        source_csv,
        "2026-08-05",
        lines=(
            "AAA,2026-08-05,100.1,101.2,105.3,99.4,104.5,4.4,4.3956,123456",
            "DIFFERENT_SYMBOL,2026-08-05,10.0,10.0,10.0,10.0,10.0,0.0,0.0,100",
        ),
    )

    result = import_local_csv_file(repo, source_csv, destination_dir=dest_dir, dry_run=False)
    assert result.action is LocalImportAction.CONFLICT
    assert result.imported is False


def test_legacy_zero_ohl_rows_valid(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_csv = tmp_path / "source" / "market_2022-07-04.csv"
    create_legacy_csv(
        source_csv,
        "2022-07-04",
        lines=(
            "ABL,2022-07-04,69.05,70.0,70.0,69.13,69.13,0.08,0.115858,50500",
            "ABL-AUG,2022-07-04,70.98,0.0,0.0,0.0,71.02,0.04,0.056353,0",
        ),
    )

    dest_dir = tmp_path / "data" / "raw"
    res = import_local_csv_file(
        repo, source_csv, destination_dir=dest_dir, dry_run=False
    )
    assert res.action is LocalImportAction.IMPORT
    assert res.imported is True
    assert res.row_count == 2


def test_unsupported_filename_ignored(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_csv = tmp_path / "source" / "unrelated_archive_data.csv"
    source_csv.parent.mkdir(parents=True)
    source_csv.write_bytes(canonical_csv_bytes((_row("AAA", 1),)))

    result = import_local_csv_file(repo, source_csv, dry_run=False)
    assert result.action is LocalImportAction.UNSUPPORTED_FILENAME
    assert result.valid is False


def test_directory_scan_non_recursive_by_default(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_dir = tmp_path / "archive"
    create_sample_csv(source_dir / "market_2026-08-07.csv")
    create_sample_csv(source_dir / "nested" / "market_2026-08-08.csv")

    dest_dir = tmp_path / "data" / "raw"

    res_default = import_local_csv_directory(
        repo, source_dir, destination_dir=dest_dir, dry_run=False, recursive=False
    )
    assert res_default.discovered_count == 1
    assert res_default.imported_count == 1

    res_recursive = import_local_csv_directory(
        repo, source_dir, destination_dir=dest_dir, dry_run=False, recursive=True
    )
    assert res_recursive.discovered_count == 2
    assert res_recursive.already_present_count == 1
    assert res_recursive.imported_count == 1


def test_second_identical_apply_produces_zero_writes_and_no_duplicates(
    tmp_path: Path,
) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_dir = tmp_path / "source"
    create_sample_csv(source_dir / "market_2026-08-07.csv")

    res1 = import_local_csv_directory(repo, source_dir, dry_run=False)
    assert res1.imported_count == 1

    res2 = import_local_csv_directory(repo, source_dir, dry_run=False)
    assert res2.imported_count == 0
    assert res2.already_present_count == 1


def test_legacy_derived_precision_normalizes_identically_to_standalone_canonical(
    tmp_path: Path,
) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_csv = tmp_path / "source" / "market_2026-08-05.csv"

    legacy_lines = (
        "786,2026-08-05,25.97,28.57,28.57,28.57,28.57,2.6000000000000014,10.011551790527538,416457",
        "AABS,2026-08-05,875.42,876.21,891.49,851.0,876.29,0.8700000000000045,0.09938086861163836,316",
        "TEST1,2026-08-05,100.00,105.00,110.00,95.00,110.00,9.995764506565012,9.995764506565012,1000",
    )
    create_legacy_csv(source_csv, "2026-08-05", lines=legacy_lines)

    dest_dir = tmp_path / "data" / "raw"
    dest_csv = dest_dir / "market_2026-08-05.csv"

    res = import_local_csv_file(repo, source_csv, destination_dir=dest_dir, dry_run=False)
    assert res.action is LocalImportAction.IMPORT
    assert res.imported is True

    dest_lines = dest_csv.read_text("utf-8").splitlines()
    assert "786,25.97,28.57,28.57,28.57,28.57,2.6,10.01,416457" in dest_lines
    assert "AABS,875.42,876.21,891.49,851,876.29,0.87,0.1,316" in dest_lines
    assert "TEST1,100,105,110,95,110,10,10,1000" in dest_lines


def test_fundamental_fields_remain_unchanged(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    source_csv = tmp_path / "source" / "market_2026-08-05.csv"

    create_legacy_csv(
        source_csv,
        "2026-08-05",
        lines=(
            "XYZ,2026-08-05,123.45,124.50,126.00,122.10,125.75,2.3000000000000007,1.8631024706358849,999",
        ),
    )

    dest_dir = tmp_path / "data" / "raw"
    dest_csv = dest_dir / "market_2026-08-05.csv"

    res = import_local_csv_file(repo, source_csv, destination_dir=dest_dir, dry_run=False)
    assert res.action is LocalImportAction.IMPORT
    assert res.imported is True

    dest_lines = dest_csv.read_text("utf-8").splitlines()
    assert dest_lines[1] == "XYZ,123.45,124.5,126,122.1,125.75,2.3,1.86,999"


def test_canonicalized_legacy_matching_existing_destination_becomes_already_present(
    tmp_path: Path,
) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    dest_dir = tmp_path / "data" / "raw"
    dest_csv = dest_dir / "market_2026-08-05.csv"

    standalone_row = ValidEquityRow(
        row_index=1,
        symbol="786",
        ldcp=Decimal("25.97"),
        open=Decimal("28.57"),
        high=Decimal("28.57"),
        low=Decimal("28.57"),
        close=Decimal("28.57"),
        change=Decimal("2.6"),
        change_percent=Decimal("10.01"),
        volume=416457,
    )
    canonical_bytes_expected = create_sample_csv(dest_csv, rows=(standalone_row,))
    expected_checksum = sha256_bytes(canonical_bytes_expected)
    repo.index_local_file(dest_csv)

    source_csv = tmp_path / "source" / "market_2026-08-05.csv"
    create_legacy_csv(
        source_csv,
        "2026-08-05",
        lines=(
            "786,2026-08-05,25.97,28.57,28.57,28.57,28.57,2.6000000000000014,10.011551790527538,416457",
        ),
    )

    res = import_local_csv_file(repo, source_csv, destination_dir=dest_dir, dry_run=False)
    assert res.action is LocalImportAction.ALREADY_PRESENT
    assert res.imported is False
    assert res.source_checksum == expected_checksum
    assert dest_csv.read_bytes() == canonical_bytes_expected


def test_genuine_difference_in_fundamental_field_remains_conflict(
    tmp_path: Path,
) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    dest_dir = tmp_path / "data" / "raw"
    dest_csv = dest_dir / "market_2026-08-05.csv"

    standalone_row = ValidEquityRow(
        row_index=1,
        symbol="786",
        ldcp=Decimal("25.97"),
        open=Decimal("28.57"),
        high=Decimal("28.57"),
        low=Decimal("28.57"),
        close=Decimal("28.57"),
        change=Decimal("2.6"),
        change_percent=Decimal("10.01"),
        volume=416457,
    )
    create_sample_csv(dest_csv, rows=(standalone_row,))
    repo.index_local_file(dest_csv)

    source_csv = tmp_path / "source" / "market_2026-08-05.csv"
    create_legacy_csv(
        source_csv,
        "2026-08-05",
        lines=(
            "786,2026-08-05,25.97,28.57,28.57,28.57,28.58,2.6,10.01,416457",
        ),
    )

    res = import_local_csv_file(repo, source_csv, destination_dir=dest_dir, dry_run=False)
    assert res.action is LocalImportAction.CONFLICT
    assert res.imported is False

