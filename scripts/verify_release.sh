#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "=== PSX Data Sync Release Verification ==="

PYTHON_BIN="${VENV_PATH:-.venv}/bin/python"

if [ ! -x "$PYTHON_BIN" ]; then
    PYTHON_BIN="python3"
fi

echo "[1/5] Running pytest full suite..."
"$PYTHON_BIN" -m pytest -q

echo "[2/5] Running pip check..."
"$PYTHON_BIN" -m pip check

echo "[3/5] Checking git diff..."
git diff --check

echo "[4/5] Checking PyInstaller bundle dist/PSX Data Sync.app..."
if [ ! -d "dist/PSX Data Sync.app" ]; then
    echo "Bundle not found, building..."
    ./scripts/build_macos.sh
fi

echo "[5/5] Verifying version resolution..."
"$PYTHON_BIN" -c "import psx_data_sync; assert psx_data_sync.__version__ == '0.5.0'; print('Version verified:', psx_data_sync.__version__)"

echo "=== All Release Verifications Passed Cleanly ==="
