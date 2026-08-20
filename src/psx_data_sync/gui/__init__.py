"""Desktop GUI package for psx-data-sync."""

from __future__ import annotations


def main() -> int:
    from .app import main as app_main

    return app_main()
