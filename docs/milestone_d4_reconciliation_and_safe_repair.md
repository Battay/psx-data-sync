# Milestone D4: Reconciliation and Safe Repair

## Objective

D4 turns the D1–D3 downloader and durable metadata into a conservative range
reconciliation engine. It detects virtual gaps, reconstructs network evidence,
checks canonical artifacts, distinguishes observations from conclusions, and
can apply a deliberately small set of audited actions without deleting or
overwriting evidence.

CSV remains the canonical market-data store. SQLite schema version 2 contains
only synchronization, evidence, repair-candidate, and decision metadata; it
does not store response bodies or OHLCV rows. D4 does not add Parquet, a GUI, a
scheduler, synthetic data, or a historical backfill.

## Policy version and evidence model

All plans, reconciliation runs, and decision events name
`psx_reconciliation_policy_v1`. A future interpretation change requires a new
identifier rather than silently reinterpreting old decisions.

For every requested calendar date the planner reconstructs:

- persistent current status and evidence provenance;
- raw attempt, valid-row, and structurally valid empty-response counts;
- distinct sync runs containing valid or empty observations;
- first/last attempt timestamps, HTTP status history, and response classes;
- weekday/weekend context and immediately adjacent healthy verified dates;
- the expected strict `market_YYYY-MM-DD.csv` path;
- current canonical validation, row count, SHA-256, and metadata consistency;
- historical verified checksum/path/row identity where it exists.

`NEVER_ATTEMPTED` is virtual during analysis: dry reconciliation does not create
one state row for every absent date. Adjacent dates are informational context,
not holiday proof.

## Non-trading classification

One HTTP 200 empty table is an observation and remains `EMPTY_UNRESOLVED`.
Retries within one sync run count as one independent observation. In policy v1,
a Saturday or Sunday becomes `CONFIRMED_NON_TRADING` only when all of these are
true:

1. at least two structurally valid PSX empty-table observations exist;
2. they came from distinct sync runs and are at least 24 hours apart;
3. there has never been a valid trading-data observation;
4. there is no historical verified identity or valid canonical CSV;
5. Saturday/Sunday calendar context supports the conclusion.

Calendar position is stored separately as `CALENDAR_SUPPORT`; it is not
misreported as a PSX observation. This threshold protects against both D1 retry
bursts and a temporary empty PSX response while allowing repeated independent
evidence plus unambiguous calendar context to resolve a weekend.

Policy v1 has no verified local official PSX holiday source. Weekdays therefore
remain `EMPTY_UNRESOLVED` even after repeated empty observations. After three
independent weekday empty observations, the action becomes `MANUAL_REVIEW`
instead of indefinite automatic polling or an unsupported holiday conclusion.

Valid trading data always wins. A later valid network observation or valid
canonical artifact overrides earlier empty evidence and a stored non-trading
conclusion. Old attempts and decision events remain immutable history.

## Status and action taxonomy

Persistent conclusions use:

- `VERIFIED_TRADING_DATA`;
- `CONFIRMED_NON_TRADING`;
- `EMPTY_UNRESOLVED`;
- `TEMPORARY_FAILURE`, `HTTP_FAILURE`, `PARSE_FAILURE`, and
  `VALIDATION_FAILURE`;
- `FILE_MISSING`, `FILE_CORRUPT`, and `FILE_CONFLICT`;
- `NEVER_ATTEMPTED` only where an operational action created a state row.

The redundant legacy `ALREADY_PRESENT_VERIFIED` state is normalized to
`VERIFIED_TRADING_DATA` during migration. Per-run download output may still use
`ALREADY_PRESENT`.

Actions are derived rather than erasing evidence:

- `NO_ACTION`;
- `NETWORK_RECHECK`;
- `LOCAL_REINDEX`;
- `REPAIR_MISSING_FILE`;
- `INVESTIGATE_CORRUPT_FILE`;
- `INVESTIGATE_CONFLICT`;
- `CONFIRM_NON_TRADING`;
- `MANUAL_REVIEW`.

## Deterministic decision order

The planner uses this conservative precedence:

1. a valid local CSV that contradicts historical SHA-256 is `FILE_CONFLICT`;
2. a valid local CSV otherwise establishes trading data and is either healthy
   or requires `LOCAL_REINDEX`;
3. an existing invalid/noncanonical file is `FILE_CORRUPT`;
4. absent canonical data with historical trading evidence is `FILE_MISSING`;
5. a contradiction-free stored non-trading conclusion remains confirmed;
6. qualifying repeated weekend empties support `CONFIRM_NON_TRADING`;
7. an absent state is a virtual `NEVER_ATTEMPTED` gap;
8. empty and failure observations remain unresolved/recheckable;
9. unsupported ambiguity requires manual review.

Artifact problems do not clear the stored last verified checksum, row count,
path, or timestamps.

## Completeness

A requested range is complete only when every inclusive calendar date is one
of:

- `VERIFIED_TRADING_DATA` with a currently valid canonical artifact whose
  checksum matches persistent identity; or
- `CONFIRMED_NON_TRADING` without contradictory trading/artifact evidence.

Every virtual gap, unresolved empty, transient/HTTP/parser/validation failure,
and missing/corrupt/conflicting artifact makes the range incomplete. Reports
show every category and label the percentage as calendar-date resolution
coverage, never trading-day coverage.

## Dry run, apply, cooldown, and concurrency

`reconcile` is dry by default. A dry run performs no HTTP request, no artifact
write, no attempt or sync-run insert, no date-state transition, and no decision
event. It writes one bounded `reconciliation_runs` audit summary, as required by
the run-history model. Missing dates remain virtual.

`--apply` enables only planned safe actions. It reuses
`ConcurrentRangeDownloader.download_dates()` for the sparse eligible target
set, with the configured bounded worker count. Healthy verified dates and
confirmed non-trading dates never enter that queue.

The default recheck budget is one actual HTTP attempt per date per
reconciliation run and is hard-capped at five. Empty, temporary, HTTP, parse,
and validation outcomes use a versioned 24-hour cooldown derived from the last
attempt. The report keeps the recommended network action visible while
separately stating whether it is eligible now and when the cooldown expires.
`--force-recheck` bypasses cooldown only for those unresolved/failure states. It
does not broaden the target set and requires `--apply`.

Short database writes remain outside HTTP critical sections. A lease table
prevents concurrent reconciliation processes from scheduling the same date;
expired claims are recoverable. Completed per-date callbacks persist before a
worker reports completion, so interruption retains finished evidence and safe
promotions. Parent reconciliation and child network runs are marked
`INTERRUPTED`, and claims are released.

## File repair and staging

Every apply-mode network result is first written beneath
`data/state/repair_staging/<reconciliation-run-id>/`. Attempt and result audit
rows point to staging while canonical date identity remains unchanged until
adjudication. Staged evidence is retained; only disposable temporary files are
cleaned.

For a missing historically verified file, automatic promotion requires:

- a still-absent canonical destination;
- a fully valid deterministic staged CSV;
- exact equality with the prior SHA-256 and valid row count;
- an atomic create-without-replacement operation.

A newly observed trading date with no historical identity may be promoted only
to an absent canonical path. The promotion copies validated bytes and uses an
atomic no-clobber link; it never uses `os.replace()` on the canonical target. A
concurrent destination wins safely and is then reconciled as evidence.

A different historical checksum/row count becomes `FILE_CONFLICT` and remains
staged for manual review. A missing prior identity cannot authorize historical
repair promotion. Existing corrupt or conflicting files are never overwritten,
deleted, or automatically selected as truth.

## Schema migration and audit history

Initialization migrates schema version 1 to version 2 inside one immediate
transaction. It adds policy/classification and cooldown metadata to date state
plus:

- `reconciliation_runs` for bounded range summaries and interruption status;
- `reconciliation_events` for important applied decisions;
- `repair_candidates` for staged validation/promotion disposition;
- `reconciliation_recheck_claims` for cross-process target leases.

Existing date state, attempts, sync runs, per-date results, identifiers,
timestamps, and checksums are retained. Metadata version is updated last,
foreign keys are checked before commit, initialization is idempotent, migration
failure rolls back, and unknown/future versions are rejected before mutation.

## CLI and scripting contract

Examples:

```bash
python -m psx_data_sync.cli reconcile --start 2026-08-01 --end 2026-08-09
python -m psx_data_sync.cli reconcile --start 2026-08-01 --end 2026-08-09 --json
python -m psx_data_sync.cli reconcile --start 2026-08-01 --end 2026-08-09 --apply
python -m psx_data_sync.cli reconcile --start 2026-08-01 --end 2026-08-09 --apply --force-recheck
```

`--only-problems` and `--status` filter displayed dates only; completeness and
summary counts always describe the full requested range. Results and JSON dates
are deterministic and date-sorted. Exit codes are `0` complete, `3` analyzed or
applied but incomplete, `2` invalid input, `1` application failure, and `130`
interrupted.

## Tests and performance

The offline suite covers the A–N evidence matrix, empty-observation
independence, trading-data override, every artifact state, dry-run isolation,
cooldown/force scope, sparse scheduling, bounded concurrency, no-clobber staged
promotion, interruption, clean JSON, exit codes, and frozen v1 migration. It
never contacts PSX.

Performance and final test-count measurements are recorded during acceptance
below rather than asserted with a fragile tight CI wall-clock threshold.

## Controlled acceptance

The D4 acceptance procedure is deliberately narrow:

1. record hashes of the four existing 2026-08-03 through 2026-08-06 artifacts;
2. migrate the real metadata database transactionally;
3. dry-reconcile 2026-08-01 through 2026-08-09 and inspect every action;
4. if a live check is needed, apply only a small unresolved subset with one
   request per date, never the full range;
5. rerun status/dry reconciliation, inspect small table counts, and verify all
   four artifact hashes are byte-identical.

Final measured results are added after those commands complete. No full
historical backfill is part of D4 acceptance.

## Limitations and D5 handoff

Policy v1 intentionally lacks an authoritative weekday holiday source and
manual approval workflow. It does not automatically resolve corrupt/conflicting
truth, clean retained staging, or schedule future runs. D5 can consume only
complete, verified canonical CSVs for a separate deterministic Parquet export;
it must preserve D4 classifications, provenance, and file immutability rather
than filling unresolved dates.
