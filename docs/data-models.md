# Data Models

[← Back to documentation index](README.md)

GenETL has no persistent data model of its own. Instead, its behaviour
is driven entirely by configuration dictionaries that are passed to
`ExtractDeleteAndLoad`. This document describes those dictionaries and
the new safe SQLAlchemy dtype mapping.

## Configuration dictionaries

All process-level configuration is provided through `config_dict`, which
groups keys by process (`download`, `delete`, `truncate`, `upload`).

### Common pattern

```python
config_dict = {
    "<process>_connections_dict": {
        "key": "<conn_type>_<conn_name>",   # references conn_dict
        ...
    },
    "<process>_sql_stmts_dict": {
        "key": "SELECT * FROM {schema}.{table} WHERE created_at >= '{cutoff}'",
        ...
    },
    "<process>_extra_vars_dict": {
        "key": {"schema": "raw", "table": "orders", "cutoff": "2026-01-01"},
        ...
    },
    "<process>_custom_conn_strs_dict": { "key": "<sqlalchemy URL or None>" },
    "<process>_connect_args_dict":    { "key": { ... } },
}
```

> **Breaking change in this release**:
> `*_extra_vars_dict` values are plain strings/numbers and are
> interpolated through `str.format`. Templates of the form
> `"eval(dt.datetime.now() - dt.timedelta(days=1))"` are **no longer
> supported**. Resolve any dynamic values in the caller before passing
> them in.

### Upload-specific keys

```python
{
    "upload_schemas_dict":  { "key": "raw" },
    "upload_tables_dict":   { "key": "orders" },
    "upload_python_to_sql_dtypes_dict": {
        "key": {
            "id":         "Integer",
            "name":       "String(255)",
            "amount":     "Numeric(12, 2)",
            "created_at": "DateTime",
        },
    },
    "upload_chunksizes_dict": { "key": 1000 },
    "upload_methods_dict":    { "key": "multi" },  # multi|execute_many|spark|single
}
```

Top-level optional keys consumed by `ExtractDeleteAndLoad`:

| Key             | Default  | Used by             |
| --------------- | -------- | ------------------- |
| `max_n_try`     | `3`      | `read_data`, `upload_data` |
| `n_parallel`    | `-1`     | `upload_data` (`n_jobs`) |
| `log_file_path` | `"logs"` | error/timing log files |

## Connection dictionary

```python
conn_dict = {
    "sqlalchemy_oltp": {
        "engine_prefix": "mssql+pyodbc",
        "server":   "...",
        "database": "...",
        "username": "...",
        "password": "...",
        "port":     1433,
    },
    "bigquery_analytics": {
        "database": "my-project.my_dataset",
        "location": "us-east1",
    },
    "redshift_dw": {
        "server":   "...",
        "database": "...",
        "username": "...",
        "password": "...",
        "port":     5439,
        "sslmode":  "require",
    },
    "oracledb_legacy": {
        "oracle_client_dir": "C:/oracle/instantclient_21_3",
        "server":   "...",
        "database": "...",
        "username": "...",
        "password": "...",
    },
    "cloudsql_pg": {
        "instance_connection_name": "project:region:instance",
        "database_type": "postgres",
        "database":      "mydb",
        "username":      "...",
        "password":      "...",
    },
}
```

Allowed `<conn_type>` prefixes (validated in
[`ExtractDeleteAndLoad`](../src/etl/edl.py)):

`sqlalchemy`, `pyodbc`, `redshift`, `oracledb`, `mysql`, `postgresql`,
`mssql`, `bigquery`, `cloudsql`.

## SQLAlchemy dtype mapping

The previous string-based mapping (`"sqlalchemy.types.String"`) is
removed because it required `eval` at runtime. The new mapping uses
SQLAlchemy classes directly.

### Default mapping

[`SQLALCHEMY_DTYPES`](../src/etl_tools/sql.py) ships with all SQLAlchemy
generic and SQL-standard types pre-registered:

```python
from etl_tools.sql import SQLALCHEMY_DTYPES
# {"String": sqlalchemy.String, "Integer": sqlalchemy.Integer, ...}
```

### Overriding or aliasing

Provide your own mapping via `sqlalchemy_dict`. Each value may be either:

* a SQLAlchemy type class / callable, or
* a fully-qualified dotted path string rooted at `sqlalchemy`, resolved
  safely via `resolve_sqlalchemy_path` (allow-listed `getattr` walk, no
  `eval`/`exec`).

```python
import sqlalchemy

# Type-class form (preferred when constructing in Python).
sqlalchemy_dict = {
    "varchar":   sqlalchemy.String,
    "timestamp": sqlalchemy.DateTime,
    "int":       sqlalchemy.Numeric,
    "float":     sqlalchemy.Float,
    "number":    sqlalchemy.Numeric,
}

# Dotted-path string form (typical over the wire / JSON payloads).
sqlalchemy_dict = {
    "varchar":   "sqlalchemy.types.String",
    "timestamp": "sqlalchemy.types.DateTime",
    "int":       "sqlalchemy.types.Integer",
    "float":     "sqlalchemy.types.Float",
    "number":    "sqlalchemy.types.Numeric",
    "bool":      "sqlalchemy.types.Boolean",
}
```

Only paths whose root is in the allow-list (`{"sqlalchemy"}`) are accepted;
private attributes (names starting with `_`) are refused.

The user-supplied dictionary is merged on top of `SQLALCHEMY_DTYPES`.

### Parameterised types

Specs like `"String(255)"` or `"Numeric(10, 2)"` are still allowed.
Arguments are parsed with `ast.literal_eval`, which only accepts
literal values (no code execution):

```python
from etl_tools.sql import resolve_sqlalchemy_dtype
resolve_sqlalchemy_dtype("String(255)")      # sqlalchemy.String(255)
resolve_sqlalchemy_dtype("Numeric(10, 2)")   # sqlalchemy.Numeric(10, 2)
```

## Logging artifacts

`etl_tools.execution` writes append-only log files under
`<log_file_path>`:

| File suffix                  | Producer            | Mode        |
| ---------------------------- | ------------------- | ----------- |
| `<name>.log`                 | `mk_exec_logs`      | append      |
| `<name>.log` (timing)        | `mk_texec_logs`     | append      |
| `<name>_summary.log`         | `mk_err_logs`       | append      |
| `<name>_detailed.log`        | `mk_err_logs`       | append      |
| `read_data_*`, `upload_data_*`, `copy_data_*` | called automatically by `sql_*` helpers | append |
