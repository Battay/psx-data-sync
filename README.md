# PSX Data Sync

PSX Data Sync is a standalone Python application and desktop GUI for downloading, validating, reconciling, and exporting historical Pakistan Stock Exchange (PSX) equity market data.

**Current Release Version:** `0.5.0`

---

## Capabilities Overview

- **Reliable Downloader Core**: Validates `YYYY-MM-DD` dates, retries transport timeouts/failures with exponential backoff and jitter, parses `tr[data-type="equity"]` rows, and writes atomic canonical CSVs.
- **Concurrent Range Synchronization**: Multi-worker asynchronous date-range downloading with configurable worker pools (1 to 16 workers).
- **Durable SQLite State & History**: Tracks attempt metadata, verification status, file checksums, and execution audit history without duplicating raw market data.
- **Evidence-Based Range Reconciliation**: Audits historical date coverage, applies non-destructive repair staging, detects file corruption or missing artifacts, and enforces evidence policy rules.
- **Derived Parquet Export**: Synchronizes date-partitioned Parquet files (`year=YYYY/month=MM/market_YYYY-MM-DD.parquet`) for fast analytical queries without mutating raw CSV artifacts.
- **Modern PySide6 Desktop GUI**: Full multi-page desktop application featuring Dashboard, Incremental Download, Local CSV Import, Range Reconciliation, Parquet Export, and Activity Logs with integrated dark theme styling and calendar date pickers.
- **macOS Application Packaging**: Standalone macOS `.app` bundle created with PyInstaller, supporting zero-dependency execution and macOS `Application Support` data storage.

---

## Installation & Setup

### Requirements
- Python 3.11 or newer (macOS, Linux, Windows)

### Development Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[test,build]"
```

The project uses a standard `src/` layout without modifying `sys.path`.

---

## Running the Application

### 1. Graphical Interface (Desktop GUI)

Launch the PySide6 desktop GUI directly from the Python virtual environment:

```bash
python -m psx_data_sync.gui.app
```

Or via CLI entrypoint:

```bash
python -m psx_data_sync.cli gui
```

### 2. Command Line Interface (CLI)

#### Fetch a Single Market Date
```bash
python -m psx_data_sync.cli fetch -d 2026-08-05
```

#### Fetch an Inclusive Date Range
```bash
python -m psx_data_sync.cli fetch-range -s 2026-08-01 -e 2026-08-05 -w 4
```

#### Inspect Synchronization Status
```bash
python -m psx_data_sync.cli status
```

#### Reconcile Date Range
```bash
python -m psx_data_sync.cli reconcile --start 2026-08-01 --end 2026-08-31
```

#### Export Parquet Partitions
```bash
python -m psx_data_sync.cli export-parquet --start 2026-08-01 --end 2026-08-31
```

---

## macOS Standalone Application Packaging

### Building the `.app` Bundle
Package PSX Data Sync into a standalone macOS Application Bundle (`dist/PSX Data Sync.app`):

```bash
./scripts/build_macos.sh
```

### Verifying the Build & Release Candidate
Run the automated release verification suite:

```bash
./scripts/verify_release.sh
```

### Creating Distributable Zip Archive
```bash
ditto -c -k --sequesterRsrc --keepParent "dist/PSX Data Sync.app" "dist/PSX-Data-Sync-0.5.0-macOS.zip"
```

---

## Runtime Data Locations

- **Development / CLI Mode**: Writable artifacts default relative to working directory:
  - State DB: `data/state/psx_sync.db`
  - Canonical CSV Data: `data/raw/market_YYYY-MM-DD.csv`
  - Parquet Export Data: `data/parquet/year=YYYY/month=MM/market_YYYY-MM-DD.parquet`
  - Repair Staging: `data/state/repair_staging/`

- **Frozen macOS Application Mode**: When launched as a standalone `.app`, data is safely stored under the user's macOS Application Support directory:
  - `~/Library/Application Support/PSX Data Sync/data/state/psx_sync.db`
  - `~/Library/Application Support/PSX Data Sync/data/raw/`
  - `~/Library/Application Support/PSX Data Sync/data/parquet/`

---

## Data Architecture & Safety Principles

1. **Canonical CSV Primacy**: UTF-8 CSV files (`symbol,ldcp,open,high,low,close,change,change_percent,volume`) remain the immutable ground truth for all market observations.
2. **Derived Parquet Synchronizer**: Parquet partitions are strictly derived downstream from canonical CSV files. If a CSV artifact changes or is updated, the Parquet synchronizer marks the partition as `STALE` and rebuilds it without modifying raw CSV data.
3. **No Fabrication Guarantee**: Missing or corrupt data is never filled with dummy or fabricated values.
4. **No-Clobber Atomic File Safety**: File writes use temporary files flushed with `fsync` followed by atomic create-without-replacement operations. Existing valid files are never overwritten automatically.
5. **Dry-Run Default**: All destructive or state-altering reconciliation and export actions default to safe dry-run preview modes requiring explicit user confirmation before applying.

---

## Automated Tests

Run the full deterministic unit test suite (325+ tests):

```bash
python -m pytest -q
python -m pip check
```

---

## License

Internal / Proprietary. All rights reserved.
