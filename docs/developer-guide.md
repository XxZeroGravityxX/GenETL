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

### Retrying CSV loads into BigQuery

`gcs_to_bigquery()` accepts an optional stable `job_id` and a job `location`:

```python
from etl_tools.gcp import gcs_to_bigquery

job = gcs_to_bigquery(
    "gs://example-bucket/exports/run-42.csv",
    "example-project",
    "analytics",
    "events",
    file_format="csv",
    write_disposition="WRITE_APPEND",
    autodetect=False,
    job_id="events_run_42",
    location="US",
    skip_leading_rows=0,
    allow_quoted_newlines=True,
    null_marker="\\N",
)
```

Reuse the ID for retries of the same logical load and keep its source object
unchanged. If BigQuery already accepted that ID, GenETL fetches the existing job
in the same project and location, verifies its source, destination and load
configuration, and waits for completion. A mismatch or failed job raises; GenETL
does not generate a replacement ID automatically. The job carries a reserved
`genetl_load_fingerprint` label to bind the request configuration to the ID.

When omitted for a deterministic load, `location` is read from the destination
dataset using the existing client. The caller therefore needs permission to read
that dataset's metadata, or must supply its location explicitly. Callers must also
be able to retrieve the original job on retries. Calls without `job_id` retain
their previous behavior; supplying `location` now forwards it to BigQuery. Job
IDs are currently supported only for CSV; other formats with an ID and unknown
CSV options fail explicitly.

### Retrying BigQuery exports to GCS

`bigquery_to_gcs()` accepts the same optional `job_id` and `location` arguments
for CSV, JSON, Parquet and Avro exports:

```python
from etl_tools.gcp import bigquery_to_gcs

job = bigquery_to_gcs(
    "example-project",
    "analytics",
    "events_snapshot_42",
    "gs://example-bucket/exports/run-42-*.parquet",
    file_format="parquet",
    job_id="events_export_42",
    location="US",
)
```

Retries reuse the original extract job after checking its source table,
destination URIs, location and extract options. They do not start another export
or conceal a failed job. Omitted locations are read from the source dataset,
requiring the same metadata-read and job-read access described above. The reserved
identity label is `genetl_extract_fingerprint`.

A reused ID refers to the original export even if the source table has changed.
Use a stable source snapshot for one logical export, and use a new ID and output
path for new data. Callers that omit `job_id` retain the SDK-generated-ID behavior.
Both helpers accept `job_id` and `location` as positional or keyword arguments
after their existing parameters.

### Other helpers

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
