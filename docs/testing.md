# Testing

[← Back to documentation index](README.md)

GenETL does not currently ship a test suite. This document outlines the
recommended testing strategy for contributors adding tests.

## Recommended stack

- **Runner**: `pytest`
- **Mocks**: `pytest-mock` for SDK boundaries (boto3, google-cloud-*,
  sqlalchemy engines)
- **Local databases**: SQLite via SQLAlchemy for fast unit tests
- **Coverage**: `pytest-cov`

Suggested layout:

```
tests/
├── unit/
│   ├── test_resolve_sqlalchemy_dtype.py
│   ├── test_extract_delete_and_load.py
│   ├── test_api_request.py
│   └── test_execution_logging.py
└── integration/
    ├── test_sqlite_roundtrip.py
    └── test_gcs_mock.py
```

## High-priority test cases

### `etl_tools.sql.resolve_sqlalchemy_dtype`

- Resolves all keys in `SQLALCHEMY_DTYPES` without arguments.
- Parses `"String(255)"`, `"Numeric(10, 2)"`, `"Enum('a', 'b')"`.
- Rejects unknown names with `ValueError`.
- Rejects argument expressions (e.g. `"String(__import__('os').system('x'))"`)
  via `ast.literal_eval`.
- Honours user-supplied mapping overrides.

### `etl.edl.ExtractDeleteAndLoad`

- Constructor rejects invalid connection types.
- Constructor rejects non-callable values in `sqlalchemy_dict`.
- `read_data` / `delete_data` / `truncate_data` template SQL with
  `str.format`, raising `KeyError` for missing placeholders.
- `read_data` propagates errors via the logger after `max_n_try`
  retries.
- `upload_data` honours the column order defined in
  `upload_python_to_sql_dtypes_dict`.

### `etl_tools.api.API_request`

- Rejects unsupported HTTP verbs.
- Raises `HTTPError` on non-2xx responses.
- Returns parsed JSON when the response is JSON; returns text otherwise.

### `etl_tools.aws.dynamodb_read_data`

- Passes kwargs through unchanged (no `eval`).
- Surfaces `botocore` errors via the logger and re-raises.

### `etl_tools.gcp` BigQuery helpers

- `bigquery_to_gcs` and `gcs_to_bigquery` surface
  `google.api_core.exceptions.GoogleAPIError` (e.g. when the table does
  not exist).

### `etl_tools.execution.setup_logger`

- Reconfiguring is idempotent (handlers replaced, not duplicated).
- Concurrent logging from threads does not produce interleaved output.

## Running tests

```bash
pip install pytest pytest-cov pytest-mock
pytest -q
pytest --cov=src --cov-report=term-missing
```
