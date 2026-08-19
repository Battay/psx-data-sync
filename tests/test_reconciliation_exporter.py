from __future__ import annotations

from datetime import date
from pathlib import Path

import psx_data_sync.exporter as exporter
from psx_data_sync.exporter import (
    StagedPromotionStatus,
    canonical_csv_bytes,
    inspect_canonical_csv_file,
    promote_staged_csv_if_safe,
    sha256_bytes,
)
from psx_data_sync.parser import parse_equity_rows
from psx_data_sync.validator import validate_rows


REQUESTED_DATE = date(2026, 8, 11)


def _canonical_stage(tmp_path: Path, fixture_bytes) -> tuple[Path, bytes]:
    rows = validate_rows(
        parse_equity_rows(fixture_bytes("valid_market.html"))
    ).valid_rows
    content = canonical_csv_bytes(rows)
    staged_path = (
        tmp_path
        / "state"
        / "repair_staging"
        / "reconcile-run"
        / f"market_{REQUESTED_DATE.isoformat()}.csv"
    )
    staged_path.parent.mkdir(parents=True)
    staged_path.write_bytes(content)
    return staged_path, content


def test_public_path_inspection_reports_canonical_identity(
    tmp_path: Path, fixture_bytes
) -> None:
    staged_path, content = _canonical_stage(tmp_path, fixture_bytes)

    inspection = inspect_canonical_csv_file(staged_path)

    assert inspection.path == staged_path
    assert inspection.exists
    assert inspection.valid
    assert inspection.row_count == 3
    assert inspection.checksum == sha256_bytes(content)


def test_public_path_inspection_checks_exact_deterministic_bytes(
    tmp_path: Path, fixture_bytes
) -> None:
    staged_path, content = _canonical_stage(tmp_path, fixture_bytes)
    staged_path.write_bytes(content.replace(b"AAA,100,", b"AAA,100.0,"))

    inspection = inspect_canonical_csv_file(staged_path)

    assert inspection.exists
    assert not inspection.valid
    assert inspection.row_count == 3
    assert inspection.checksum == sha256_bytes(staged_path.read_bytes())
    assert "not canonical" in (inspection.error or "")


def test_historical_repair_promotes_only_an_exact_identity_match(
    tmp_path: Path, fixture_bytes
) -> None:
    staged_path, content = _canonical_stage(tmp_path, fixture_bytes)
    destination = tmp_path / "raw" / staged_path.name

    result = promote_staged_csv_if_safe(
        staged_path,
        destination,
        expected_checksum=sha256_bytes(content),
        expected_row_count=3,
    )

    assert result.status is StagedPromotionStatus.PROMOTED
    assert result.promoted
    assert result.checksum == sha256_bytes(content)
    assert result.row_count == 3
    assert destination.read_bytes() == content
    assert staged_path.read_bytes() == content

    # The retained evidence and canonical artifact do not share an inode.
    staged_path.write_bytes(b"retained evidence may later be annotated")
    assert destination.read_bytes() == content


def test_historical_checksum_or_row_mismatch_is_not_promoted(
    tmp_path: Path, fixture_bytes
) -> None:
    staged_path, content = _canonical_stage(tmp_path, fixture_bytes)
    checksum = sha256_bytes(content)
    checksum_target = tmp_path / "raw" / "checksum-mismatch.csv"
    row_target = tmp_path / "raw" / "row-mismatch.csv"

    checksum_result = promote_staged_csv_if_safe(
        staged_path,
        checksum_target,
        expected_checksum="0" * 64,
        expected_row_count=3,
    )
    row_result = promote_staged_csv_if_safe(
        staged_path,
        row_target,
        expected_checksum=checksum,
        expected_row_count=4,
    )

    assert checksum_result.status is StagedPromotionStatus.HISTORICAL_MISMATCH
    assert row_result.status is StagedPromotionStatus.HISTORICAL_MISMATCH
    assert not checksum_target.exists()
    assert not row_target.exists()
    assert staged_path.read_bytes() == content


def test_new_gap_requires_explicit_policy_permission(
    tmp_path: Path, fixture_bytes
) -> None:
    staged_path, content = _canonical_stage(tmp_path, fixture_bytes)
    rejected_target = tmp_path / "raw" / "rejected.csv"
    allowed_target = tmp_path / "raw" / "allowed.csv"

    rejected = promote_staged_csv_if_safe(staged_path, rejected_target)
    promoted = promote_staged_csv_if_safe(
        staged_path,
        allowed_target,
        allow_new=True,
    )

    assert rejected.status is StagedPromotionStatus.POLICY_REJECTED
    assert not rejected_target.exists()
    assert promoted.status is StagedPromotionStatus.PROMOTED
    assert allowed_target.read_bytes() == content
    assert staged_path.read_bytes() == content


def test_partial_historical_identity_is_rejected(
    tmp_path: Path, fixture_bytes
) -> None:
    staged_path, content = _canonical_stage(tmp_path, fixture_bytes)
    destination = tmp_path / "raw" / staged_path.name

    result = promote_staged_csv_if_safe(
        staged_path,
        destination,
        expected_checksum=sha256_bytes(content),
    )

    assert result.status is StagedPromotionStatus.POLICY_REJECTED
    assert not destination.exists()
    assert staged_path.read_bytes() == content


def test_invalid_staged_data_is_never_promoted(tmp_path: Path) -> None:
    staged_path = tmp_path / "repair_staging" / "market_2026-08-11.csv"
    staged_path.parent.mkdir()
    staged_path.write_bytes(b"corrupt\n")
    destination = tmp_path / "raw" / staged_path.name

    result = promote_staged_csv_if_safe(
        staged_path,
        destination,
        allow_new=True,
    )

    assert result.status is StagedPromotionStatus.STAGED_FILE_INVALID
    assert not destination.exists()
    assert staged_path.read_bytes() == b"corrupt\n"


def test_existing_destination_is_never_overwritten(
    tmp_path: Path, fixture_bytes
) -> None:
    staged_path, content = _canonical_stage(tmp_path, fixture_bytes)
    destination = tmp_path / "raw" / staged_path.name
    destination.parent.mkdir()
    destination.write_bytes(b"pre-existing evidence")

    result = promote_staged_csv_if_safe(
        staged_path,
        destination,
        allow_new=True,
    )

    assert result.status is StagedPromotionStatus.DESTINATION_ALREADY_EXISTS
    assert destination.read_bytes() == b"pre-existing evidence"
    assert staged_path.read_bytes() == content


def test_concurrent_destination_winner_is_never_overwritten(
    tmp_path: Path, fixture_bytes, monkeypatch
) -> None:
    staged_path, content = _canonical_stage(tmp_path, fixture_bytes)
    destination = tmp_path / "raw" / staged_path.name
    concurrent_content = b"concurrent writer won"
    real_link = exporter.os.link

    def race_link(source: Path, target: Path) -> None:
        Path(target).write_bytes(concurrent_content)
        real_link(source, target)

    monkeypatch.setattr(exporter.os, "link", race_link)

    result = promote_staged_csv_if_safe(
        staged_path,
        destination,
        allow_new=True,
    )

    assert result.status is StagedPromotionStatus.DESTINATION_ALREADY_EXISTS
    assert destination.read_bytes() == concurrent_content
    assert staged_path.read_bytes() == content
    assert not list(destination.parent.glob("*.promotion.tmp"))
