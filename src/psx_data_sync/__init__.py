"""PSX Data Sync package."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("psx-data-sync")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"
