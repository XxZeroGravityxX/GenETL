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
  and uses `logger.info/warning/error` instead of `print()`. The
  configuration is performed once by the caller via
  [`setup_logger`](../src/etl_tools/execution.py).
- **Security**: never call `eval()` on configuration values. Use
  `str.format` for SQL templating and
  [`resolve_sqlalchemy_dtype`](../src/etl_tools/sql.py) for dtype
  specs.
- **Errors**: client/connector errors must be caught at the SDK boundary,
  logged with full context, and re-raised so callers can react.

## Development workflow

1. Create a feature branch from `main`.
2. Add/modify code under `src/`.
3. Add or update unit tests (see [testing.md](testing.md)).
4. Run the test suite locally.
5. Update the relevant docs in `docs/` if behaviour or APIs changed.
6. Bump the version in `pyproject.toml` if you publish a release.
7. Open a pull request.

## Configuring the logger

The library never installs its own log handlers. In your entrypoint:

```python
from etl_tools.execution import setup_logger

logger = setup_logger()                       # INFO by default
# or
import logging
logger = setup_logger(level=logging.DEBUG)
```

Every `etl_tools.*` and `etl.*` module then routes through the same
queue-based handler that `setup_logger` installs.
