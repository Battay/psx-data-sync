"""Independent row validation for parsed PSX equity observations."""

from __future__ import annotations

from collections.abc import Iterable

from .state import (
    ParsedEquityRow,
    RejectedRow,
    ValidationResult,
    ValidEquityRow,
)


def _deduplicate(items: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(items))


def validate_rows(rows: Iterable[ParsedEquityRow]) -> ValidationResult:
    """Validate rows independently so one malformed source row cannot spoil a date."""

    valid_rows: list[ValidEquityRow] = []
    rejected_rows: list[RejectedRow] = []
    seen_symbols: set[str] = set()

    for row in rows:
        reasons = list(row.parse_errors)
        symbol = row.symbol.strip()
        if not symbol:
            reasons.append("symbol is empty")

        numeric_values = (
            row.ldcp,
            row.open,
            row.high,
            row.low,
            row.close,
            row.change,
            row.change_percent,
        )
        if not reasons and (any(value is None for value in numeric_values) or row.volume is None):
            reasons.append("one or more required numeric fields are invalid")

        if not reasons:
            assert row.ldcp is not None
            assert row.open is not None
            assert row.high is not None
            assert row.low is not None
            assert row.close is not None
            assert row.change is not None
            assert row.change_percent is not None
            assert row.volume is not None

            if row.ldcp < 0:
                reasons.append("ldcp cannot be negative")
            if row.open < 0:
                reasons.append("open cannot be negative")
            if row.high < 0:
                reasons.append("high cannot be negative")
            if row.low < 0:
                reasons.append("low cannot be negative")
            if row.close <= 0:
                reasons.append("close must be positive")
            if row.volume < 0:
                reasons.append("volume cannot be negative")
            if row.high > 0 and row.low > 0 and row.high < row.low:
                reasons.append("high cannot be below low")

            symbol_key = symbol.casefold()
            if symbol_key in seen_symbols:
                reasons.append("duplicate symbol")

        if reasons:
            rejected_rows.append(
                RejectedRow(
                    row_index=row.row_index,
                    symbol=symbol,
                    raw_values=row.raw_values,
                    reasons=_deduplicate(reasons),
                )
            )
            continue

        seen_symbols.add(symbol.casefold())
        valid_rows.append(
            ValidEquityRow(
                row_index=row.row_index,
                symbol=symbol,
                ldcp=row.ldcp,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                change=row.change,
                change_percent=row.change_percent,
                volume=row.volume,
            )
        )

    return ValidationResult(tuple(valid_rows), tuple(rejected_rows))
