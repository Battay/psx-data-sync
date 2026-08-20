#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "=== Building PSX Data Sync standalone macOS application ==="

PYTHON_BIN="${VENV_PATH:-.venv}/bin/python"

if [ ! -x "$PYTHON_BIN" ]; then
    PYTHON_BIN="python3"
fi

"$PYTHON_BIN" -m PyInstaller --noconfirm --clean psx_data_sync.spec

echo "=== Build Complete ==="
echo "Application bundle output: dist/PSX Data Sync.app"
