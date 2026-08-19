from __future__ import annotations

from psx_data_sync.parser import parse_equity_rows, parse_field_values
from psx_data_sync.validator import validate_rows


def make_row(*values: str):
    return parse_field_values(values, row_index=1)


def test_literal_null_is_rejected(fixture_bytes) -> None:
    result = validate_rows(parse_equity_rows(fixture_bytes("null_row.html")))

    assert not result.valid_rows
    assert "open is null" in result.rejected_rows[0].reasons


def test_negative_price_is_rejected() -> None:
    result = validate_rows(
        [make_row("BAD", "10", "-1", "11", "9", "10", "0", "0", "10")]
    )

    assert "open cannot be negative" in result.rejected_rows[0].reasons


def test_zero_open_high_low_are_allowed_with_positive_close() -> None:
    result = validate_rows(
        [make_row("ZERO", "10", "0", "0", "0", "10", "0", "0", "0")]
    )

    assert len(result.valid_rows) == 1
    assert not result.rejected_rows


def test_close_must_be_positive() -> None:
    result = validate_rows(
        [make_row("ZERO", "10", "0", "0", "0", "0", "0", "0", "0")]
    )

    assert "close must be positive" in result.rejected_rows[0].reasons


def test_negative_volume_is_rejected() -> None:
    result = validate_rows(
        [make_row("BAD", "10", "10", "11", "9", "10", "0", "0", "-1")]
    )

    assert "volume cannot be negative" in result.rejected_rows[0].reasons


def test_malformed_numeric_row_does_not_fail_valid_rows(fixture_bytes) -> None:
    rows = list(parse_equity_rows(fixture_bytes("valid_market.html")))
    rows.extend(parse_equity_rows(fixture_bytes("malformed_numeric.html")))

    result = validate_rows(rows)

    assert len(result.valid_rows) == 3
    assert len(result.rejected_rows) == 1
    assert "open is not numeric" in result.rejected_rows[0].reasons[0]


def test_duplicate_symbol_is_rejected_individually(fixture_bytes) -> None:
    result = validate_rows(parse_equity_rows(fixture_bytes("duplicate_symbol.html")))

    assert len(result.valid_rows) == 1
    assert result.rejected_rows[0].reasons == ("duplicate symbol",)


def test_wrong_field_count_is_rejected() -> None:
    result = validate_rows([parse_field_values(["SHORT", "1"], row_index=4)])

    assert not result.valid_rows
    assert result.rejected_rows[0].reasons == ("expected 9 fields, found 2",)
