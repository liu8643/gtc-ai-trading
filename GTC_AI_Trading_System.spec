# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path
from PyInstaller.utils.hooks import collect_submodules

# R5N5 FIX:
# - Add lxml / html5lib / bs4 for pandas.read_html() and MOPS HTML table fallback.
# - Keep yfinance hidden imports.
# - Include data/charts only when folders exist to avoid GitHub Actions packaging failure.

hiddenimports = []
for pkg in ("yfinance", "lxml", "html5lib", "bs4"):
    try:
        hiddenimports += collect_submodules(pkg)
    except Exception:
        hiddenimports.append(pkg)

hiddenimports += [
    "xlrd",
    "openpyxl",
    "lxml.etree",
    "lxml.html",
    "html5lib",
    "bs4",
]

datas = []
if Path("data").exists():
    datas.append(("data", "data"))
if Path("charts").exists():
    datas.append(("charts", "charts"))

a = Analysis(
    ["main.py"],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=sorted(set(hiddenimports)),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="main",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
)
