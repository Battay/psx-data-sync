"""PSX HTML response classification and equity-row parsing."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Sequence

from bs4 import BeautifulSoup

from .state import ContentClassification, ParsedEquityRow


NUMERIC_FIELDS: tuple[str, ...] = (
    "ldcp",
    "open",
    "high",
    "low",
    "close",
    "change",
    "change_percent",
)
NUMERIC_PATTERN = re.compile(
    r"^[+-]?(?:(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d*)?|\.\d+)$"
)


def _as_text(content: bytes | str) -> str:
    if isinstance(content, bytes):
        return content.decode("utf-8", errors="replace")
    return content


def classify_html(content: bytes | str) -> ContentClassification:
    """Classify content structurally; byte length is deliberately not decisive."""

    text = _as_text(content).strip()
    if not text:
        return ContentClassification.NON_HTML

    soup = BeautifulSoup(text, "html.parser")
    if soup.select_one('tr[data-type="equity"]') is not None:
        return ContentClassification.EQUITY_ROWS
    for table in soup.find_all("table"):
        header_text = {
            " ".join(cell.get_text(" ", strip=True).casefold().split())
            for cell in table.find_all("th")
        }
        required_headers = {"symbol", "ldcp", "open", "high", "low", "close", "volume"}
        if required_headers.issubset(header_text):
            return ContentClassification.EMPTY_MARKET_RESPONSE
    if soup.find() is not None:
        return ContentClassification.MALFORMED_HTML
    return ContentClassification.NON_HTML


def parse_decimal(raw: str, field_name: str) -> tuple[Decimal | None, str | None]:
    value = raw.strip()
    if not value:
        return None, f"{field_name} is empty"
    if value.casefold() == "null":
        return None, f"{field_name} is null"

    if field_name == "change_percent" and value.endswith("%"):
        value = value[:-1].strip()
        if not value:
            return None, "change_percent is empty"

    if NUMERIC_PATTERN.fullmatch(value) is None:
        return None, f"{field_name} is not numeric: {value!r}"

    normalized = value.replace(",", "")
    try:
        parsed = Decimal(normalized)
    except InvalidOperation:
        return None, f"{field_name} is not numeric: {value!r}"
    if not parsed.is_finite():
        return None, f"{field_name} is not finite"
    return parsed, None


def parse_volume(raw: str) -> tuple[int | None, str | None]:
    value, error = parse_decimal(raw, "volume")
    if error is not None or value is None:
        return None, error
    if value != value.to_integral_value():
        return None, "volume must be an integer"
    return int(value), None


def parse_field_values(
    values: Sequence[str], row_index: int
) -> ParsedEquityRow:
    """Parse one nine-field row while retaining errors for row-level rejection."""

    raw_values = tuple(value.strip() for value in values)
    symbol = raw_values[0] if raw_values else ""
    if len(raw_values) != 9:
        return ParsedEquityRow(
            row_index=row_index,
            raw_values=raw_values,
            symbol=symbol,
            ldcp=None,
            open=None,
            high=None,
            low=None,
            close=None,
            change=None,
            change_percent=None,
            volume=None,
            parse_errors=(f"expected 9 fields, found {len(raw_values)}",),
        )

    parsed_numbers: list[Decimal | None] = []
    errors: list[str] = []
    for field_name, raw in zip(NUMERIC_FIELDS, raw_values[1:8], strict=True):
        number, error = parse_decimal(raw, field_name)
        parsed_numbers.append(number)
        if error is not None:
            errors.append(error)

    volume, volume_error = parse_volume(raw_values[8])
    if volume_error is not None:
        errors.append(volume_error)

    return ParsedEquityRow(
        row_index=row_index,
        raw_values=raw_values,
        symbol=symbol.strip(),
        ldcp=parsed_numbers[0],
        open=parsed_numbers[1],
        high=parsed_numbers[2],
        low=parsed_numbers[3],
        close=parsed_numbers[4],
        change=parsed_numbers[5],
        change_percent=parsed_numbers[6],
        volume=volume,
        parse_errors=tuple(errors),
    )


def parse_equity_rows(content: bytes | str) -> tuple[ParsedEquityRow, ...]:
    """Extract only ``tr[data-type=equity]`` rows from a PSX response."""

    soup = BeautifulSoup(_as_text(content), "html.parser")
    parsed: list[ParsedEquityRow] = []
    for row_index, row in enumerate(
        soup.select('tr[data-type="equity"]'), start=1
    ):
        cells = row.find_all("td")
        values = [cell.get_text(" ", strip=True) for cell in cells]
        parsed.append(parse_field_values(values, row_index))
    return tuple(parsed)
