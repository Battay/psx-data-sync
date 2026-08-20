# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_data_files, collect_submodules, copy_metadata

block_cipher = None

datas = copy_metadata("psx-data-sync")

hiddenimports = [
    "psx_data_sync",
    "psx_data_sync.config",
    "psx_data_sync.downloader",
    "psx_data_sync.exporter",
    "psx_data_sync.importer",
    "psx_data_sync.parquet_sync",
    "psx_data_sync.reconciliation",
    "psx_data_sync.state",
    "psx_data_sync.state_db",
    "psx_data_sync.gui",
    "psx_data_sync.gui.app",
    "psx_data_sync.gui.main_window",
    "psx_data_sync.gui.dashboard",
    "psx_data_sync.gui.download_panel",
    "psx_data_sync.gui.import_panel",
    "psx_data_sync.gui.reconciliation_panel",
    "psx_data_sync.gui.parquet_panel",
    "psx_data_sync.gui.logs_panel",
    "psx_data_sync.gui.theme",
    "psx_data_sync.gui.workers",
    "psx_data_sync.gui.widgets",
    "psx_data_sync.gui.widgets.date_edit",
    "pandas",
    "pyarrow",
    "pyarrow.parquet",
    "sqlite3",
    "bs4",
    "httpx",
    "rich",
    "typer",
    "tenacity",
]

a = Analysis(
    ['src/psx_data_sync/gui/app.py'],
    pathex=['src'],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['pytest', 'unittest'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='PSX Data Sync',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='PSX Data Sync',
)

app = BUNDLE(
    coll,
    name='PSX Data Sync.app',
    icon=None,
    bundle_identifier='pk.com.psx.datasync',
)
