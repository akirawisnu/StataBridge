# StataBridge
> Universal Stata Data Converter — portable Windows CLI

[![Version](https://img.shields.io/badge/version-1.0.0-blue)]()
[![Platform](https://img.shields.io/badge/platform-Windows%207%2F8%2F10%2F11-blue)]()
[![License](https://img.shields.io/badge/license-MIT-green)]()
[![Stata](https://img.shields.io/badge/Stata-7%E2%80%9319-informational)]()

StataBridge is a portable Windows CLI tool for research teams working across multiple Stata
versions. Drop the `.exe` anywhere and run — no Python, no installation, no dependencies.

---

## What it does

Convert `.dta` files across **all 13 Stata versions** (7 through 19) and bridge to/from
four additional formats used in academic and applied research:

| Format | Read | Write |
|--------|------|-------|
| Stata .dta (all versions) | ✓ | ✓ |
| CSV | ✓ | ✓ |
| XLSX / XLS | ✓ | ✓ |
| DBF (dBASE III/IV) | ✓ | ✓ |
| RData / .rda / .rds | ✓ | — |

---

## Requirements

StataBridge ships as a **portable Windows executable** (`.exe`).

- Windows 7, 8, 10, or 11 — **64-bit**
- No Python installation required
- No additional dependencies

> Reading `.RData` / `.rda` / `.rds` files requires R 4.0+ installed on the system.

---

## Usage

Double-click `stata_bridge.exe` or run it from a terminal:

```
stata_bridge.exe
```

The interactive menu guides you through:

1. Language selection
2. Conversion mode — single file / multiple files / batch folder (with optional recursion)
3. Input file or folder path (drag & drop supported)
4. Output format
5. Target Stata version (when writing `.dta`)
6. Output directory (blank = same folder as input)
7. Overwrite confirmation

---

## Conversion modes

| Mode | Description |
|------|-------------|
| Single file | One input → one output |
| Multiple files | Comma-separated paths or glob patterns (`*.dta`) |
| Batch folder | All supported files in a folder |
| Batch + subfolders | Recursive, preserves directory structure |

---

## Stata version support

| Target | Format written | Key characteristics |
|--------|---------------|---------------------|
| Stata 7–12 | 114 | Latin-1, str ≤ 244 chars |
| Stata 13 | 117 | strL support, Latin-1 |
| Stata 14–15 | 118 | Unicode (UTF-8), strL |
| Stata 16–19 | 119 | Unicode, 32,767+ variables |

### Automatic safeguards

- **strL truncation** — long strings clipped to 244 chars (format 114) or 2045 (format 117+)
  with a per-column warning
- **Unicode → Latin-1 fallback** — unmappable characters replaced when targeting old formats
- **Column name sanitization** — illegal Stata names fixed automatically
- **Metadata preservation** — variable & value labels carried through Stata ↔ Stata conversions

---

## UI languages

English · Indonesian · Vietnamese · German · French · Spanish · Italian · Portuguese · Japanese · Chinese

---

## Files

```
StataBridge/
├── stata_bridge.exe               # Portable application — run directly
└── StataBridge_Documentation.md  # Quick-start guide
```

---

## License

MIT — free for academic and commercial use.

---

*Built at the University of Göttingen · @akirawisnu · April 2026*
