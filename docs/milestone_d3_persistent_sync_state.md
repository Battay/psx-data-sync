# Milestone D3: Persistent Synchronization State

## Objective

D3 makes the D1/D2 downloader durable and resumable without changing CSV's role
as the canonical market-data artifact. A local SQLite database records current
per-date state, every completed network attempt, and every synchronization run.
It stores metadata only, not OHLCV rows.

The default database is `data/state/psx_sync.db`; override it with
`PSX_STATE_DB_PATH`.

## Schema version 1

`sync_schema_metadata` holds the explicit schema and application versions.
Initialization is idempotent. A database with an unknown version, including a
future version, is rejected rather than silently mutated.

The operational tables are:

- `date_sync_state`: one current row per market date, including evidence state,
  lifetime counters, last HTTP/result details, row counts, checksum, relative
  CSV path, UTC timestamps, and concise errors;
- `download_attempts`: immutable audit rows for individual network attempts,
  unique by run/date/attempt number;
- `sync_runs`: invocation identity, requested range, worker count, timing,
  aggregate counts, interruption flag, and application version;
- `sync_run_date_results`: one final outcome per completed date in each run.

Indexes cover current status, attempt date, attempt run, result date, and run
start time. Attempts/results use foreign keys to runs and date state without
destructive cascades. SQLite runs with foreign keys enabled, WAL journaling,
full synchronous durability, a busy timeout, and transactional writes.

## Status taxonomy and transitions

The canonical persistent enum distinguishes:

- `NEVER_ATTEMPTED`;
- `VERIFIED_TRADING_DATA` and `ALREADY_PRESENT_VERIFIED`;
- `EMPTY_UNRESOLVED`;
- `TEMPORARY_FAILURE`, `HTTP_FAILURE`, `PARSE_FAILURE`, and
  `VALIDATION_FAILURE`;
- `FILE_MISSING`, `FILE_CORRUPT`, and `FILE_CONFLICT`.

All mutations pass through a centralized transition function. Unresolved or
failed dates can become verified after a later successful fetch. A transient
failure cannot erase verified identity or successful artifact metadata.
`ALREADY_PRESENT_VERIFIED` does not downgrade `VERIFIED_TRADING_DATA`. Only an
explicit local artifact check can move verified state to missing, corrupt, or
conflicting.

Empty HTTP responses remain `EMPTY_UNRESOLVED`, regardless of repetition. D3 has
no evidence model sufficient to label a permanent non-trading day.

## Attempt and run history

Each completed HTTP attempt records UTC start/end timestamps, duration, status,
byte count, structural response classification, retryability, concise error,
row counts, checksum/path when created, and worker identity. Response HTML and
stack traces are not stored.

Every single-date and range invocation creates a run. Final run summaries are
derived from durable attempt and per-date result rows rather than transient
in-memory counters. A cancelled range is finalized as `INTERRUPTED`; results
already written remain available to its successor.

## Concurrency and connection lifetime

HTTP fetching retains D2's bounded concurrency and shared pooled client. The
async state façade uses one `asyncio.Lock` and sends each small SQLite operation
through `asyncio.to_thread`. Repository methods open a short-lived connection,
run one transaction, and close it. Connections are never shared across tasks or
threads, and database serialization does not surround HTTP work.

## CSV/database consistency model

The success ordering is response, parse, validation, atomic CSV save, checksum,
then transactional metadata commit. If metadata persistence fails after a CSV
save, the file is retained; the next preflight validates and indexes it locally.

Preflight implements these rules:

1. verified state plus a matching canonical CSV skips HTTP;
2. verified state plus a missing CSV records `FILE_MISSING` and permits recovery;
3. verified state plus different valid canonical bytes records `FILE_CONFLICT`;
4. malformed/noncanonical existing content records `FILE_CORRUPT`;
5. a valid CSV without state is verified and indexed locally;
6. unresolved/failure state without a valid CSV remains network eligible.

The last verified checksum is retained when corruption or conflict is detected.
No mismatch is deleted or overwritten automatically.

## Bootstrap and inspection

`python -m psx_data_sync.cli state-bootstrap` scans strict
`market_YYYY-MM-DD.csv` names, validates every row and deterministic formatting,
computes SHA-256, counts rows, and stores repository-relative paths. It performs
zero HTTP calls and is idempotent. Re-running it does not inflate attempt counts
or create attempt history.

`status`, `status --date`, and `status --start/--end` query SQLite only. They
provide aggregate status counts or per-date artifact, timing, error, and recent
attempt details.

## Resume semantics

Per-date results are persisted before a worker reports completion. On
interruption, the run records the completed subset and the interrupted attempt
when cancellation can be observed. A later identical range validates completed
files locally and schedules HTTP only for missing, failed, or unresolved dates.
Results remain date-sorted even though work completes concurrently.

## Tests and measured overhead

The offline suite covers schema safety, transitions, lifetime counters, attempt
uniqueness, runs, bootstrap idempotence, every state-aware preflight case,
checksum corruption, CLI views, mixed ranges, zero-network ranges, interruption
and resume, and 20 concurrent state-writing dates. The concurrency stress test
confirms four HTTP requests remain active while database writes serialize.

On the acceptance machine, a temporary local benchmark measured approximately:

- date-state lookup: 0.15 ms;
- final date-state/result transaction: 0.31 ms;
- attempt insert plus date-state update: 0.30 ms;
- 100-file bootstrap: about 1,348 files/s.

The complete deterministic suite passed 100 tests. These values
are operational measurements, not cross-machine performance guarantees.

## Live acceptance

Acceptance on 2026-08-19 used only the existing August files, one zero-network
repeat, and one nearby uncached date:

- `state-bootstrap` discovered and indexed three valid files with zero invalid:
  2026-08-03 (589 rows,
  `94a267ca50de0bfc6ba57982debb5265a2d53106fef31fe994de046581e45053`),
  2026-08-04 (596 rows,
  `b1caea7d11ab48e43eb7e2953172a4330887a40dd6725e36aaf1f53858c6071f`),
  and 2026-08-05 (596 rows,
  `ca014be7a63fc1372482ac2637c692061c8fdc3acfbd50de14e9009a271a22e5`);
- the 2026-08-03 through 2026-08-05 repeat used an intentionally unreachable
  configured endpoint and still completed with three local skips, zero attempts,
  zero network dates, and zero response bytes (run
  `d02790dca69c4f6faa15e0d9689043ab`);
- extending only through 2026-08-06 skipped the three known dates and made one
  successful PSX attempt for the uncached date. It stored 590 rows with checksum
  `ce1d4adc3b87f5d263b602b818d6c7b3cd215a96f4b1734374946f22ab028a95`
  (run `1316a2727ac34c00bd47e93047950bcd`);
- final state tracked four `VERIFIED_TRADING_DATA` dates and no unresolved,
  failed, missing, corrupt, or conflicting dates. The new date's audit row
  records HTTP 200, `EQUITY_ROWS`, one attempt, and
  `NETWORK_VALIDATED_CSV` evidence.

No broad historical backfill was performed.

## Remaining limitations and D4 handoff

D3 does not reconcile empty observations against calendars, adjacent trading
evidence, or official sources. It does not repair/delete conflicts, schedule
runs, export Parquet, or provide a GUI. D4 should consume the conservative
`EMPTY_UNRESOLVED` and artifact inconsistency states, add evidence-aware
reconciliation, and keep D3's audit and non-destructive guarantees.
