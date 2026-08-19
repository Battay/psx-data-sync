# PSX Data Sync

PSX Data Sync is a standalone Python application for downloading historical
Pakistan Stock Exchange equity data from the official PSX data portal. Milestone
D1 provides a reliable, synchronous downloader for exactly one requested date.

## Current capability: D1

The D1 pipeline:

1. validates one strict `YYYY-MM-DD` date and rejects future dates;
2. sends a form-encoded `POST` request to
   `https://dps.psx.com.pk/historical`;
3. retries bounded transport, timeout, HTTP 429/5xx, and suspicious content
   failures with exponential backoff and jitter;
4. classifies the returned HTML by structure, parses only
   `tr[data-type="equity"]`, and validates each row independently;
5. writes valid data atomically to a canonical CSV and calculates its SHA-256
   checksum.

D1 intentionally does not download ranges or run concurrent workers.

## Installation

Python 3.11 or newer is required. From the repository root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e .
```

Install the offline test dependencies when needed:

```bash
pip install -e ".[test]"
```

The project uses a `src/` package layout and standard editable installation; it
does not modify `sys.path`.

## Fetch one date

```bash
python -m psx_data_sync.cli fetch --date 2026-08-05
```

The short option is equivalent:

```bash
python -m psx_data_sync.cli fetch -d 2026-08-05
```

Successful data is saved as `data/raw/market_YYYY-MM-DD.csv`. The terminal
summary includes the response classification, HTTP status, attempt count, row
counts, output path, checksum, and timing breakdown.

## Canonical output

CSV files are UTF-8, contain no dataframe index, are sorted by symbol, and use
this fixed column order:

```text
symbol,ldcp,open,high,low,close,change,change_percent,volume
```

Numeric output is normalized without inventing or imputing values. Negative
change fields are preserved. Literal `null`, malformed, non-finite, or otherwise
invalid required values reject only their source row. Zero open/high/low values
are permitted when supplied by PSX, but close must be positive.

## Reliability and file safety

An HTTP 200 response is not assumed to contain market data. PSX can return a
valid empty table temporarily, so classification examines HTML structure rather
than relying on response size. Even a final empty response is reported
conservatively as `NON_TRADING_OR_EMPTY`; it is **not** treated as proof of a
holiday or confirmed non-trading day.

Writes use a temporary file, file flush and `fsync`, followed by an atomic rename.
Existing output is validated before comparison:

- identical valid content is reported as `ALREADY_PRESENT`;
- invalid existing content is left untouched and reported as
  `EXISTING_FILE_INVALID`;
- valid but different content is left untouched and reported as `FILE_CONFLICT`;
- empty, malformed, failed, or fully rejected downloads never overwrite a file.

No market observations are fabricated.

## Configuration

Defaults work without a `.env` file. Straightforward overrides are available:

- `PSX_HISTORICAL_URL`
- `PSX_REQUEST_TIMEOUT_SECONDS`
- `PSX_CONNECT_TIMEOUT_SECONDS`
- `PSX_RETRY_ATTEMPTS`
- `PSX_RETRY_BACKOFF_INITIAL_SECONDS`
- `PSX_RETRY_BACKOFF_MAX_SECONDS`
- `PSX_RETRY_JITTER_FRACTION`
- `PSX_USER_AGENT`
- `PSX_RAW_OUTPUT_DIR`

## Tests

All automated tests are deterministic and use synthetic HTML plus mocked HTTP;
pytest never requires live PSX access.

```bash
python -m pytest -v
python -m pip check
```

## Roadmap

- D2: concurrent date-range downloader
- D3: synchronization state
- D4: reconciliation
- D5: Parquet export workflow
- D6: graphical interface
