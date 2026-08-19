# PSX Data Sync

PSX Data Sync is a standalone Python application for downloading historical
Pakistan Stock Exchange equity data from the official PSX data portal. Milestone
D1 provides a reliable single-date core, D2 adds bounded concurrent downloads,
and D3 adds durable, resumable synchronization metadata in SQLite. D4 adds
evidence-based range reconciliation, gap detection, cooldown-aware targeted
rechecks, and no-clobber staged repair.

## Current capability: D1 through D4

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

Before either fetch command contacts PSX, it checks persistent state and validates
the local canonical file, including its SHA-256 checksum. A matching verified
file is reported as `ALREADY_PRESENT` with zero HTTP attempts. Dates with
unresolved empty responses or temporary, HTTP, parse, and validation failures
remain eligible for later retry.

Ranges longer than 365 dates are allowed for automation but produce a warning.
No full multi-year backfill is performed automatically.

## Persistent synchronization state

Synchronization metadata is stored by default in
`data/state/psx_sync.db`. CSV files remain the canonical market-data artifacts;
SQLite stores only status, attempt and run history, HTTP/result metadata, row
counts, checksums, relative file paths, timestamps, and concise errors. It does
not duplicate OHLCV observations.

The version-2 schema uses WAL mode, foreign keys, and full synchronous writes.
Initialization transactionally migrates version 1 without dropping operational
tables or history and rejects unknown future versions.
Network work remains concurrent, while each short SQLite operation uses its own
connection and asynchronous range writes pass through one lock and worker
thread. This avoids sharing connections across tasks or blocking the event loop.

The conservative persistent states are:

- `VERIFIED_TRADING_DATA` (the legacy `ALREADY_PRESENT_VERIFIED` alias is
  normalized during migration);
- `EMPTY_UNRESOLVED`;
- `TEMPORARY_FAILURE`, `HTTP_FAILURE`, `PARSE_FAILURE`, and
  `VALIDATION_FAILURE`;
- `FILE_MISSING`, `FILE_CORRUPT`, and `FILE_CONFLICT`;
- `CONFIRMED_NON_TRADING`, only for a conclusion supported by the named,
  versioned reconciliation policy;
- `NEVER_ATTEMPTED` for a newly created state row.

An individual empty PSX response is always `EMPTY_UNRESOLVED`. An observation is
not treated as a holiday conclusion.

### Index existing CSV files

After installing D3, index existing canonical files without any network calls:

```bash
python -m psx_data_sync.cli state-bootstrap
```

Bootstrap scans `data/raw/market_YYYY-MM-DD.csv`, validates canonical content,
counts rows, and computes SHA-256. It is idempotent: repeated runs do not increase
attempt counters or create audit attempts. Invalid files are reported and left
untouched.

### Inspect state

```bash
python -m psx_data_sync.cli status
python -m psx_data_sync.cli status --date 2026-08-05
python -m psx_data_sync.cli status --start 2026-08-01 --end 2026-08-31
```

These commands never contact PSX. The date view includes lifetime attempts,
row counts, checksum, relative CSV path, timestamps, the last error, and recent
network attempts.

### Resume and artifact consistency

Each `fetch` and `fetch-range` invocation gets a durable run identity. Completed
date results are committed as workers finish. If a range is interrupted, its run
is marked `INTERRUPTED`; the next invocation verifies and locally skips completed
dates, then retries only the missing or unresolved work.

If state says a CSV was verified but it is now missing, the date is marked
`FILE_MISSING`. Ordinary `fetch` and `fetch-range` stop locally with
`REPAIR_REQUIRED`, perform zero HTTP attempts for that date, and direct the
operator to `reconcile --apply`. Only that explicit workflow may network-recover
a trusted identity, and it downloads into repair staging before audited
adjudication. A malformed file becomes `FILE_CORRUPT`; a valid canonical file
whose checksum differs from the last verified checksum becomes `FILE_CONFLICT`.
Corrupt and conflicting files are never overwritten or deleted automatically.

## Reconcile a range

Reconciliation is a dry run by default and makes no HTTP requests:

```bash
python -m psx_data_sync.cli reconcile \
  --start 2026-08-01 \
  --end 2026-08-09
```

The report evaluates every inclusive calendar date, including weekends. It
shows reconstructed evidence, local file/checksum health, the projected
classification, and a canonical action. `--only-problems` and `--status` are
presentation-only filters; `--json` produces a deterministic automation report
without progress output.

Safe actions and targeted rechecks require explicit apply mode:

```bash
python -m psx_data_sync.cli reconcile \
  --start 2026-08-01 \
  --end 2026-08-09 \
  --apply
```

Only dates whose plan calls for a network recheck and whose cooldown has elapsed
are sent to the existing bounded D2 downloader. `--force-recheck` can bypass a
cooldown only for unresolved empty/network/parser/validation failure states; it
does not target healthy verified data, confirmed non-trading dates, corrupt
files, or checksum conflicts. It is valid only with `--apply`.

### Reconciliation policy v1

Every report and decision event names
`psx_reconciliation_policy_v1`. Its conservative rules are:

- one empty response never proves a non-trading date;
- retries within one sync run count as one observation, not independent
  evidence;
- a Saturday or Sunday may become `CONFIRMED_NON_TRADING` only after at least
  two structurally valid empty PSX observations from distinct runs at least 24
  hours apart, with no trading observation or valid canonical artifact;
- weekend position is supporting calendar evidence, never a network
  observation by itself;
- weekdays remain `EMPTY_UNRESOLVED` because this project has no verified local
  official PSX holiday calendar; after three independent empty observations the
  action becomes `MANUAL_REVIEW`;
- any valid trading-data observation or valid canonical artifact overrides an
  earlier empty/non-trading conclusion.

Adjacent verified dates are reported as context only. They are not proof that a
date was a holiday.

### Completeness and exit codes

A range is `COMPLETE` only when every calendar date resolves to either verified
trading data with a healthy canonical file or confirmed non-trading with no
contradictory artifact. Never-attempted, empty-unresolved, failed, missing,
corrupt, and conflicting dates make it `INCOMPLETE`. The percentage is labeled
calendar-date resolution coverage rather than trading-day coverage.

`reconcile` exits `0` for a complete range, `3` for a successfully rendered but
incomplete range, `2` for invalid input, `1` for a configuration/database/
orchestration failure, and `130` when interrupted.

### Repair staging and immutability

Apply-mode downloads first write to a unique directory beneath
`data/state/repair_staging/<reconciliation-run-id>/`. Staged evidence is
retained. A missing historical artifact is automatically promoted only when its
strictly validated canonical bytes match both the prior SHA-256 and row count.
A newly observed trading date without historical identity may be promoted only
to an absent canonical path. Promotion uses an atomic create-without-replacement
operation, so a concurrent file is never overwritten.

Checksum differences become `FILE_CONFLICT`; malformed existing files remain
`FILE_CORRUPT`. Neither is overwritten, deleted, or selected as truth
automatically. Reconciliation attempts, candidates, runs, and important
decision events remain auditable in SQLite without storing response bodies or
OHLCV data.

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

Writes use a flushed and `fsync`ed temporary file followed by an atomic
create-without-replacement hard link. A destination created concurrently is
treated as the winner and is inspected; it is never replaced. Existing output
is validated before comparison:

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
- `PSX_STATE_DB_PATH`
- `PSX_REPAIR_STAGING_DIR`
- `PSX_MAX_RECHECKS_PER_DATE_PER_RUN` (default 1, hard-capped at 5)
- `PSX_RECONCILIATION_COOLDOWN_SECONDS` (default 86400)

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
- D2: concurrent date-range downloader — done
- D3: persistent synchronization state — done
- D4: reconciliation and safe repair — done
- D5: Parquet export workflow
- D6: graphical interface
- D7: benchmark, package, and release
