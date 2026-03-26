# AGENTS Guide

Repository guidance for coding agents working in `jpxstockdatadl`.

## Project Overview

- Language: Python
- Packaging: `pyproject.toml` with `hatchling`
- Source root: `src/jpxstockdatadl/`
- Tests: `unittest` in `tests/test_downloader.py`
- Main modules:
  - `src/jpxstockdatadl/main.py` - CLI entry
  - `src/jpxstockdatadl/downloader.py` - core download/export logic
  - `src/jpxstockdatadl/helper.py` - small XBRL helper utilities

## External Instruction Files

- No `AGENTS.md` existed before this one.
- No `.cursor/rules/` directory found.
- No `.cursorrules` file found.
- No `.github/copilot-instructions.md` file found.
- Follow this file and the existing repository code style.

## Environment Notes

- Real CLI runs need `edinet_xbrl`, `lxml`, and `EDINET_API_KEY`.
- Tests stub external modules, so most unit tests run without those runtime dependencies.
- This machine may use an externally managed Python, so prefer a virtual environment for installs.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -U pip
python3 -m pip install -e .
python3 -m pip install build
```

## Build Commands

- Build wheel and sdist:

```bash
python3 -m build
```

- Syntax check Python files:

```bash
python3 -m py_compile src/jpxstockdatadl/*.py tests/*.py
```

## Test Commands

- Run the full test module:

```bash
python3 -m unittest tests.test_downloader
```

- Run one test class:

```bash
python3 -m unittest tests.test_downloader.DownloaderSessionCacheTests
```

- Run one test method:

```bash
python3 -m unittest tests.test_downloader.DownloaderSessionCacheTests.test_build_json_output_path_uses_date_and_filing_type
```

- Fast syntax validation after edits:

```bash
python3 -m py_compile src/jpxstockdatadl/*.py tests/*.py
```

## CLI Commands

- Installed package:

```bash
jpxstockdatadl 6136
jpxstockdatadl 6136 --years 3
```

- Run from source:

```bash
PYTHONPATH=src python3 -m jpxstockdatadl 6136 --years 3
```

## Linting and Formatting

- No repo-configured `ruff`, `black`, `isort`, `flake8`, or `mypy` setup exists.
- Do not add a formatter or mass-reformat files unless the user asks.
- Match surrounding formatting and keep diffs small.
- Use tests plus `py_compile` as the minimum validation baseline.

## Code Style

### Imports

- Start modules with `from __future__ import annotations`.
- Group imports as: standard library, third-party, local package imports.
- Prefer explicit imports.
- Keep import blocks stable unless the change requires otherwise.

### Formatting

- Use 4-space indentation.
- Follow existing PEP 8-ish layout.
- Use double quotes consistently.
- Preserve blank lines between top-level definitions.
- Avoid unnecessary line wrapping churn.

### Types

- Add type hints to public functions and most helpers.
- Prefer modern built-in generics like `list[str]` and `dict[str, Any]`.
- Use `str | None` style unions.
- Use `Any` only at JSON or third-party boundaries.

### Naming

- Functions and variables: `snake_case`
- Classes/dataclasses: `PascalCase`
- Module constants: `UPPER_SNAKE_CASE`
- Regex constants should end in `_RE`

### Data Handling

- Prefer `Path` over raw path strings.
- Use frozen dataclasses when modeling stable internal records.
- Validate untyped JSON-like data with `isinstance` checks before use.
- Keep helper functions small and composable.

### Error Handling

- Raise `ValueError` for invalid inputs and malformed state.
- Let ordinary filesystem failures surface as `OSError` unless a wrapper adds value.
- Catch narrow exception groups near ZIP, XML, network, and file IO boundaries.
- In batch export/download flows, record failures and continue when possible.
- CLI-facing errors should be short, printed to `stderr`, and end with exit status 1.

### Output Conventions

- Write text with `encoding="utf-8"`.
- Keep JSON readable with `json.dumps(..., ensure_ascii=False, indent=2)`.
- Be careful with filename conventions; downstream logic depends on them.
- Preserve manifest/session-cache compatibility when editing cached data behavior.

## Testing Conventions

- Use `unittest`, not `pytest`-only features.
- Use `tempfile.TemporaryDirectory()` and `Path` for file-based tests.
- Stub external imports with `types.ModuleType` when avoiding real dependencies.
- Keep tests offline and deterministic.
- Add focused tests when changing export formatting, cache behavior, or error handling.

## Repository-Specific Guidance

- `downloader.py` is the central module; avoid broad refactors unless necessary.
- Export order matters; review `finalize_download_summary()` before changing exports.
- Preserve cache/session semantics in manifest handling.
- Ignore non-filing JSON files when aggregating exported financial data.
- Keep new helpers near the related download/export code.

## Before Finishing

- Run the most targeted unit test for the change.
- Run `python3 -m unittest tests.test_downloader` for behavioral changes.
- Run `python3 -m py_compile src/jpxstockdatadl/*.py tests/*.py` after Python edits.
- If build or runtime commands cannot run because dependencies are missing, say exactly what is missing.
