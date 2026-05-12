# Security

[← Back to documentation index](README.md)

This document captures the threat model and the hardening changes made
in the current release. It is required reading before contributing
changes that touch configuration parsing, SQL templating, or anything
that crosses a trust boundary.

## Threat model

GenETL is a library executed inside the caller's process. The most
likely abuse vectors are:

1. **Code injection through configuration** — configuration
   dictionaries may originate from YAML/JSON files, environment
   variables or remote sources. Any use of `eval()` over such values is
   a remote code execution vulnerability.
2. **SQL injection** — string-interpolated SQL templates may carry
   attacker-controlled values (table names, filter values).
3. **Credential leakage** — connection dictionaries hold usernames,
   passwords and tokens; these must never appear in logs or error
   messages.

## Changes in this release

### Removed all `eval()` usage

| Previous location                         | Replacement                                  |
| ----------------------------------------- | -------------------------------------------- |
| `etl.edl` SQL template `eval(stmt)`       | `str.format_map(_StrictDict(extra_vars))`    |
| `etl.edl` extra-vars `eval(ex_val)`       | Values passed through as literals            |
| `etl.edl` sqlalchemy dtype `eval(...)`    | [`SQLALCHEMY_DTYPES`](../src/etl_tools/sql.py) + `ast.literal_eval` |
| `etl_tools.aws.dynamodb_read_data` `eval(val)` | Pass kwargs straight through to boto3 |

### Safer SQL dispatch

- `mode` strings are validated against
  `_CONN_FACTORIES`/`_ENGINE_FACTORIES`; unknown modes raise
  `ValueError` instead of producing a `NameError` deep in the call
  stack.
- `sql_exec_stmt` now re-raises after the retry path fails, so failures
  cannot be silently absorbed into a `(0, None)` return.

### Better error surfacing

- BigQuery extract/load jobs raise on
  `google.api_core.exceptions.GoogleAPIError` (covers missing tables,
  permission denied, bad SQL, etc.) and log
  `extract_job.errors`/`load_job.errors` when present.
- Cloud SQL Admin API import/export calls raise on
  `googleapiclient.errors.HttpError`, and propagate operation-level
  errors as `RuntimeError`.
- DynamoDB scans/uploads catch `BotoCoreError`/`ClientError`, log them
  with context, and re-raise.
- The generic `API_request` helper now calls `raise_for_status()` and
  logs the failing body before propagating.

### Logging

- Every module owns a `logger = logging.getLogger(__name__)`.
- `print()` is no longer used in `sql.py`, `gcp.py`, `aws.py`, `api.py`,
  `edl.py`, or `execution.py`.
- The shared
  [`setup_logger`](../src/etl_tools/execution.py) helper uses a
  `QueueHandler` + locked `StreamHandler` so concurrent workers cannot
  interleave their output.

## Hardening guidance for callers

### 1. Never feed untrusted strings into SQL templates

`str.format`-based templating still produces literal SQL. Bind values
through SQLAlchemy parameters or quote them server-side if they could be
attacker-controlled:

```python
# Bad
config_dict["download_sql_stmts_dict"]["orders"] = (
    "SELECT * FROM orders WHERE customer = '{customer}'"
)

# Better: pass the value as a SQLAlchemy bound parameter
from sqlalchemy import text
stmt = text("SELECT * FROM orders WHERE customer = :customer")
```

### 2. Redact credentials before logging

Connection dictionaries are passed verbatim to engine factories; do not
log them. If you need to log connection metadata, redact sensitive keys
(e.g. `password`, `secret`, `token`) similarly to
`common.modules.tools.api.auxiliar.print_secure_conn` in
`CAS_datascience_services`.

### 3. Use environment-scoped credentials

Prefer Application Default Credentials (GCP) and IAM roles (AWS) over
embedding long-lived secrets in `conn_dict`. When secrets are
unavoidable, source them from a secret manager (GCP Secret/Parameter
Manager, AWS Secrets Manager, HashiCorp Vault) and inject them at
runtime.

### 4. Validate `mode`, `method` and connection types

These values are validated by the library, but callers should restrict
the set of values they accept at the configuration layer (e.g. enums in
Pydantic models) to fail early.

### 5. Pin dependencies

[`requirements.txt`](../requirements.txt) pins versions for the critical
SDKs. Re-pinning after security advisories
(`pip-audit`/`pip install --upgrade`) is part of normal maintenance.

## Known limitations

- The Redshift `COPY` statement in
  [`sql_copy_data`](../src/etl_tools/sql.py) interpolates credentials
  into the SQL string. This is unavoidable for the
  `COPY ... ACCESS_KEY_ID '...' SECRET_ACCESS_KEY '...'` syntax;
  callers must ensure the values come from a trusted source.
- Cloud SQL `import_context`/`export_context` accept arbitrary kwargs
  that are forwarded directly to the Admin API. Validate inputs before
  passing them in.
