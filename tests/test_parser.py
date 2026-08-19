from decimal import Decimal

from psx_data_sync.parser import (
    classify_html,
    parse_decimal,
    parse_equity_rows,
    parse_field_values,
)
from psx_data_sync.state import ContentClassification


def test_classifier_detects_equity_response(fixture_bytes) -> None:
    assert (
        classify_html(fixture_bytes("valid_market.html"))
        is ContentClassification.EQUITY_ROWS
    )


def test_classifier_detects_empty_shell_by_structure_at_903_bytes(
    fixture_bytes,
) -> None:
    shell = fixture_bytes("empty_shell.html")
    assert len(shell) < 903
    padded_shell = shell + b" " * (903 - len(shell))

    assert len(padded_shell) == 903
    assert (
        classify_html(padded_shell)
        is ContentClassification.EMPTY_MARKET_RESPONSE
    )
    assert (
        classify_html(shell + b" " * 2000)
        is ContentClassification.EMPTY_MARKET_RESPONSE
    )


def test_classifier_detects_malformed_and_non_html(fixture_bytes) -> None:
    assert (
        classify_html(fixture_bytes("malformed.html"))
        is ContentClassification.MALFORMED_HTML
    )
    assert classify_html(b'{"error":"upstream"}') is ContentClassification.NON_HTML
    assert (
        classify_html(b"<html><table><tr><td>unrelated</td></tr></table></html>")
        is ContentClassification.MALFORMED_HTML
    )


def test_parser_extracts_exact_fields_and_numeric_signs(fixture_bytes) -> None:
    rows = parse_equity_rows(fixture_bytes("valid_market.html"))

    assert len(rows) == 3
    aaa = next(row for row in rows if row.symbol == "AAA")
    bbb = next(row for row in rows if row.symbol == "BBB")
    assert len(aaa.raw_values) == 9
    assert aaa.open == Decimal("101.00")
    assert aaa.change == Decimal("3.00")
    assert aaa.change_percent == Decimal("3.00")
    assert aaa.volume == 1_234_567
    assert bbb.change == Decimal("-2")
    assert bbb.change_percent == Decimal("-4.00")


def test_parser_preserves_bad_row_for_validation() -> None:
    row = parse_field_values(["SHORT", "1", "2"], row_index=7)

    assert row.row_index == 7
    assert row.parse_errors == ("expected 9 fields, found 3",)


def test_malformed_thousands_separator_is_not_silently_accepted() -> None:
    value, error = parse_decimal("1,,234", "open")

    assert value is None
    assert error == "open is not numeric: '1,,234'"
