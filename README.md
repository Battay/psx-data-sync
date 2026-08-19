# PSX Data Sync

PSX Data Sync is a standalone Python application for downloading historical
Pakistan Stock Exchange equity data from the official PSX data portal. Milestone
D1 provides a reliable single-date core, and Milestone D2 adds bounded concurrent
downloads for inclusive date ranges.

## Current capability: D1 and D2

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

The D2 range pipeline reuses that exact parsing, validation, checksum, and atomic
export path. It schedules a bounded number of asynchronous workers over one
shared, connection-pooled HTTP session. A failure or retry backoff for one date
does not terminate or block unrelated dates.

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

## Fetch an inclusive range

```bash
python -m psx_data_sync.cli fetch-range \
  --start 2026-08-01 \
  --end 2026-08-05 \
  --workers 4
```

Short options are supported:

```bash
python -m psx_data_sync.cli fetch-range \
  -s 2026-08-01 \
  -e 2026-08-05 \
  -w 4
```

Both endpoints are included, and weekends are not skipped. The default is four
workers; accepted values are 1 through 16. D2 creates only that many worker tasks
and uses a small bounded queue, so the number of active requests cannot exceed
the configured worker count.

The command displays a Rich progress bar followed by deterministic date-sorted
outcomes and aggregate counts, retries, bytes, rows, duration, and throughput.
One date's timeout, server error, malformed response, or unresolved empty table
does not discard other dates' results.

Before a range worker contacts PSX, it validates any existing canonical file for
that date and recalculates its SHA-256 checksum. A strictly canonical file is
reported as `ALREADY_PRESENT` with zero HTTP attempts. Invalid or noncanonical
files are never overwritten. The single-date `fetch` command retains D1 behavior
and still contacts PSX before comparing downloaded content.

Ranges longer than 365 dates are allowed for automation but produce a warning.
Persistent multi-year backfill state and reconciliation belong to later
milestones.

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
- `PSX_RANGE_WORKERS`
- `PSX_MAX_RANGE_WORKERS` (hard-capped at 16)
- `PSX_LARGE_RANGE_WARNING_DAYS`
- `PSX_USER_AGENT`
- `PSX_RAW_OUTPUT_DIR`

## Tests

All automated tests are deterministic and use synthetic HTML plus mocked HTTP;
pytest never requires live PSX access.

```bash
python -m pytest -v
python -m pip check
```

For controlled small-range performance experiments, the library provides
`benchmark_worker_counts`, which compares workers 1, 2, and 4 without dropping
failed dates. Callers should give each run equivalent isolated output state so
local-file skips do not distort later measurements.

## Roadmap

- D1: single-date core — done
- D2: concurrent date-range downloader — done, pending acceptance
- D3: synchronization state
- D4: reconciliation and repair
- D5: Parquet export workflow
- D6: graphical interface
- D7: benchmark, package, and release
