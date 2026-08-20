from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from psx_data_sync import __version__


def test_package_version() -> None:
    try:
        expected = version("psx-data-sync")
    except PackageNotFoundError:
        expected = "0.0.0+unknown"
    assert __version__ == expected
