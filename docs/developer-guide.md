# Developer Guide

[← Back to documentation index](README.md)

## Project layout

```
GenETL/
├── build/                  # Build helpers (build_pkg.py, build.bat, build.sh)
├── docs/                   # This documentation
├── src/
│   ├── etl/
│   │   ├── __init__.py
│   │   └── edl.py          # ExtractDeleteAndLoad orchestrator
│   └── etl_tools/
│       ├── __init__.py
│       ├── api.py          # HTTP wrapper
│       ├── aws.py          # S3 / DynamoDB
│       ├── execution.py    # Logging, parallel exec, subprocess
│       ├── gcp.py          # GCS / BigQuery / Cloud SQL
│       └── sql.py          # SQL engines, helpers and dtype mapping
├── LICENSE
├── pyproject.toml
├── README.md
└── requirements.txt
```

## Setup

```bash
# Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1     # Windows PowerShell
source .venv/bin/activate      # Linux / macOS

# Install dependencies
pip install -r requirements.txt

# Install the package in editable mode for local development
pip install -e .
```

## Build commands

The repository ships convenience scripts in `build/`:

```bash
# Build a source distribution + wheel using build_pkg.py
python build/build_pkg.py

# Or use the OS-specific wrappers
build/build.sh     # Linux / macOS
build\build.bat    # Windows
```

## Code conventions

- **Style**: PEP 8, 4-space indentation, double-quoted strings, type
  hints on public APIs.
- **Comments**: hierarchical block comments (`#`, `##`, `###`)
  mirroring the structure used in
  `CAS_datascience_services/svc/ETL/app/src/main.py`. The pattern is
  applied throughout the rewritten files.
- **Imports**: standard library first, third party second, local last;
  each group separated by a blank line.
- **Logging**: every module owns a `logger = logging.getLogger(__name__)`
  and uses `logger.info/warning/error` instead of `print()`. The host
  application (e.g. FastAPI service) must configure root logging via
  its own logger setup (see
  `CAS_datascience_services/svc/ETL/app/src/main.py` line 49 for an example).
- **Security**: never call `eval()` on configuration values. Use
  `str.format` for SQL templating and
  [`resolve_sqlalchemy_dtype`](../src/etl_tools/sql.py) for dtype
  specs.
- **Errors**: client/connector errors must be caught at the SDK boundary,
  logged with full context, and re-raised so callers can react.

## Error handling

All high-level helpers (`sql_read_data`, `sql_upload_data`, `sql_copy_data`,
`read_data`, `upload_data`, etc.) now re-raise exceptions after logging.
Callers should wrap these in try/except blocks to handle failures gracefully.

Previously, these functions would retry on error and silently return empty
results (0 rows, empty DataFrame) on final failure. Now they raise after
exhausting retries, which allows proper error handling at the endpoint or
application level.

```python
from etl import ExtractDeleteAndLoad

edl = ExtractDeleteAndLoad(config_dict, conn_dict)
try:
    edl.read_data()
except RuntimeError as e:
    # Handle the error (log, send alert, return HTTP 500, etc.)
    print(f"Failed to read data: {e}")
```
