# GenETL Documentation

Welcome to the GenETL documentation. GenETL is a Python library that
implements reusable, **secure-by-default** ETL building blocks for
extracting, deleting and loading data across heterogeneous storage
backends (relational databases, BigQuery, Cloud SQL, S3, GCS, DynamoDB,
HTTP APIs).

## Documentation index

### For developers

- [Developer Guide](developer-guide.md) — setup, workflows and conventions
- [Architecture](architecture.md) — system design and components
- [Testing](testing.md) — testing strategies and patterns

### For integration

- [Data Models](data-models.md) — configuration dictionaries and dtype mapping

### For operations

- [Security](security.md) — threat model, hardening notes and secure usage

## Project overview

GenETL exposes a single high-level class, `ExtractDeleteAndLoad`
(`etl.edl.ExtractDeleteAndLoad`), that orchestrates four processes
configured through dictionaries: `download`, `delete`, `truncate`, and
`upload`. The class delegates to lower-level helpers exposed in
`etl_tools`:

| Module                | Responsibility                                            |
| --------------------- | --------------------------------------------------------- |
| [src/etl/edl.py](../src/etl/edl.py)                 | High-level orchestrator class                          |
| [src/etl_tools/sql.py](../src/etl_tools/sql.py)     | SQL engines, connections, read/upload helpers, dtype mapping |
| [src/etl_tools/gcp.py](../src/etl_tools/gcp.py)     | GCS / BigQuery / Cloud SQL helpers                     |
| [src/etl_tools/aws.py](../src/etl_tools/aws.py)     | S3 / DynamoDB helpers                                  |
| [src/etl_tools/api.py](../src/etl_tools/api.py)     | Generic HTTP helper                                    |
| [src/etl_tools/execution.py](../src/etl_tools/execution.py) | Logging, parallel execution, subprocess helpers |

## Technology stack

- **Language**: Python `>= 3.9`
- **Data**: pandas `2.2.x`, numpy `1.26.x`
- **SQL**: SQLAlchemy `2.0.x`, pyodbc, oracledb, redshift_connector,
  psycopg2, cloud-sql-python-connector
- **GCP**: google-cloud-storage, google-cloud-bigquery, sqlalchemy-bigquery,
  google-api-python-client
- **AWS**: boto3, awswrangler
- **HTTP**: requests
- **Big data**: pyspark (optional, for the Spark/Redshift upload path)

## External resources

- Source: <https://github.com/XxZeroGravityxX/GenETL>
- PyPI: <https://pypi.org/project/GenETL>
