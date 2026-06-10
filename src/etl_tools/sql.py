# Import modules
import ast
import datetime as dt
import importlib
import logging
import multiprocessing
import os
import sys
import traceback

# Import third-party modules
import numpy as np
import pandas as pd
import pyodbc
import pyspark as ps
import redshift_connector
import oracledb
import sqlalchemy

# Import submodules
from sqlalchemy import create_engine
from sqlalchemy.schema import DDL

# Import custom modules
from etl_tools.execution import mk_err_logs, mk_texec_logs, parallel_execute


# Module-level logger
logger = logging.getLogger(__name__)


# ============================================================================
# Safe SQLAlchemy dtype mapping
# ============================================================================

#: Default mapping from string aliases to SQLAlchemy type classes.
#:
#: This mapping replaces the previous ``eval()``-based dtype resolution. Users
#: may extend or override entries by passing their own dictionary to
#: :class:`etl.edl.ExtractDeleteAndLoad` via the ``sqlalchemy_dict`` argument.
#: Values may be either:
#:
#: * a SQLAlchemy type class / callable (e.g. ``sqlalchemy.String``), or
#: * a fully-qualified dotted path string rooted at an allow-listed module
#:   (currently only ``"sqlalchemy"``), e.g. ``"sqlalchemy.types.String"``.
#:   String values are resolved via :func:`resolve_sqlalchemy_path` using a
#:   safe attribute walk (no ``eval``/``exec``).
SQLALCHEMY_DTYPES: dict[str, type] = {
    # Generic types
    "BigInteger": sqlalchemy.BigInteger,
    "Boolean": sqlalchemy.Boolean,
    "Date": sqlalchemy.Date,
    "DateTime": sqlalchemy.DateTime,
    "Enum": sqlalchemy.Enum,
    "Float": sqlalchemy.Float,
    "Integer": sqlalchemy.Integer,
    "Interval": sqlalchemy.Interval,
    "LargeBinary": sqlalchemy.LargeBinary,
    "Numeric": sqlalchemy.Numeric,
    "PickleType": sqlalchemy.PickleType,
    "SmallInteger": sqlalchemy.SmallInteger,
    "String": sqlalchemy.String,
    "Text": sqlalchemy.Text,
    "Time": sqlalchemy.Time,
    "Unicode": sqlalchemy.Unicode,
    "UnicodeText": sqlalchemy.UnicodeText,
    "JSON": sqlalchemy.JSON,
    # SQL standard types
    "ARRAY": sqlalchemy.ARRAY,
    "BIGINT": sqlalchemy.BIGINT,
    "BINARY": sqlalchemy.BINARY,
    "BLOB": sqlalchemy.BLOB,
    "BOOLEAN": sqlalchemy.BOOLEAN,
    "CHAR": sqlalchemy.CHAR,
    "CLOB": sqlalchemy.CLOB,
    "DATE": sqlalchemy.DATE,
    "DATETIME": sqlalchemy.DATETIME,
    "DECIMAL": sqlalchemy.DECIMAL,
    "FLOAT": sqlalchemy.FLOAT,
    "INTEGER": sqlalchemy.INTEGER,
    "NCHAR": sqlalchemy.NCHAR,
    "NUMERIC": sqlalchemy.NUMERIC,
    "NVARCHAR": sqlalchemy.NVARCHAR,
    "REAL": sqlalchemy.REAL,
    "SMALLINT": sqlalchemy.SMALLINT,
    "TEXT": sqlalchemy.TEXT,
    "TIME": sqlalchemy.TIME,
    "TIMESTAMP": sqlalchemy.TIMESTAMP,
    "VARBINARY": sqlalchemy.VARBINARY,
    "VARCHAR": sqlalchemy.VARCHAR,
}


#: Module roots that :func:`resolve_sqlalchemy_path` is allowed to traverse.
#:
#: Keeping this set explicit prevents the resolver from being abused to import
#: arbitrary modules from string input (e.g. ``"os.system"``).
_ALLOWED_DTYPE_ROOTS: frozenset[str] = frozenset({"sqlalchemy"})


def resolve_sqlalchemy_path(path: str):
    """
    Resolve a fully-qualified dotted path string (e.g.
    ``"sqlalchemy.types.String"``) into the actual Python object, using a
    safe allow-listed attribute walk.

    This is the eval-free replacement for ``eval(path)``. It only accepts
    paths whose root is listed in :data:`_ALLOWED_DTYPE_ROOTS` (currently just
    ``"sqlalchemy"``) and refuses any private attribute access.

    Parameters:
        path (str): Dotted path rooted at an allow-listed top-level module.

    Returns:
        Any: The resolved attribute (typically a SQLAlchemy type class).

    Raises:
        ValueError: If ``path`` is not a non-empty string, its root is not
                    allow-listed, references a private attribute, or any
                    attribute along the path does not exist.
    """
    if not isinstance(path, str) or not path.strip():
        raise ValueError(f"Invalid dotted dtype path: {path!r}")

    parts = path.strip().split(".")
    root = parts[0]
    if root not in _ALLOWED_DTYPE_ROOTS:
        raise ValueError(
            f"Refusing to resolve '{path}': root '{root}' is not allow-listed. "
            f"Allowed roots: {sorted(_ALLOWED_DTYPE_ROOTS)}"
        )

    try:
        obj = importlib.import_module(root)
    except ImportError as exc:  # pragma: no cover - sqlalchemy is a hard dep
        raise ValueError(
            f"Could not import root module '{root}' while resolving '{path}'"
        ) from exc

    for attr in parts[1:]:
        if not attr or attr.startswith("_"):
            raise ValueError(
                f"Refusing private/empty attribute '{attr}' in '{path}'"
            )
        try:
            obj = getattr(obj, attr)
        except AttributeError as exc:
            raise ValueError(
                f"Unknown attribute '{attr}' while resolving '{path}'"
            ) from exc

    return obj


def resolve_type_class(
    name: str,
    mapping: dict[str, type] | None = None,
) -> type:
    """
    Central resolver: turn a type name into a SQLAlchemy type **class**.

    Resolution order:

    1. Exact lookup of ``name`` in ``mapping`` (defaults to :data:`SQLALCHEMY_DTYPES`).
    2. Case-insensitive lookup in ``mapping`` for simple (non-dotted) names.
    3. If ``name`` contains a dot, attempt :func:`resolve_sqlalchemy_path`
       (e.g. ``"sqlalchemy.types.String"``). Dotted paths remain case-sensitive
       because Python attribute access is case-sensitive.
    4. Otherwise raise :class:`ValueError`.

    Parameters:
        name (str): Simple key (e.g. ``"String"``) or dotted path
                    (e.g. ``"sqlalchemy.types.String"``).
        mapping (dict[str, type] | None): Optional override mapping. Defaults
                    to :data:`SQLALCHEMY_DTYPES`.

    Returns:
        type: SQLAlchemy type class (not yet instantiated).

    Raises:
        ValueError: If the name cannot be resolved through any strategy.
    """
    if mapping is None:
        mapping = SQLALCHEMY_DTYPES

    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"Invalid SQLAlchemy type name: {name!r}")

    name = name.strip()

    # 1. Exact mapping lookup
    if name in mapping:
        return mapping[name]

    # 1b. Case-insensitive mapping lookup (simple names only; dotted paths keep
    #     their original casing because Python attribute access is case-sensitive)
    if "." not in name:
        name_lower = name.lower()
        for k, v in mapping.items():
            if k.lower() == name_lower:
                return v

    # 2. Dotted-path fallback (still case-sensitive — Python attrs are)
    if "." in name:
        try:
            resolved = resolve_sqlalchemy_path(name)
        except ValueError:
            raise ValueError(
                f"Unknown SQLAlchemy dtype '{name}'. Not found in mapping "
                f"and could not be resolved as a dotted path. "
                f"Available mapping keys: {sorted(mapping.keys())}"
            )
        if not callable(resolved):
            raise ValueError(
                f"Resolved path '{name}' is not a callable type class."
            )
        return resolved

    raise ValueError(
        f"Unknown SQLAlchemy dtype '{name}'. "
        f"Available keys: {sorted(mapping.keys())}"
    )


def resolve_sqlalchemy_dtype(
    spec: str,
    mapping: dict[str, type] | None = None,
):
    """
    Resolve a string spec such as ``"String"`` or ``"String(255)"`` into a
    SQLAlchemy type instance, using a safe mapping and ``ast.literal_eval``.

    The type class is resolved centrally via :func:`resolve_type_class`, which
    supports both simple mapping keys (e.g. ``"String"``) and fully-qualified
    dotted paths (e.g. ``"sqlalchemy.types.String"``).

    Parameters:
        spec (str): Dtype spec. May include parenthesised literal arguments
                    (e.g. ``"Numeric(10, 2)"`` or ``"String(255)"``).
        mapping (dict[str, type] | None): Optional override mapping. Defaults
                    to :data:`SQLALCHEMY_DTYPES`.

    Returns:
        sqlalchemy.types.TypeEngine: Instantiated SQLAlchemy type.

    Raises:
        ValueError: If ``spec`` is not a string or refers to an unknown key.
    """
    if mapping is None:
        mapping = SQLALCHEMY_DTYPES

    if not isinstance(spec, str) or not spec.strip():
        raise ValueError(f"Invalid SQLAlchemy dtype spec: {spec!r}")

    spec = spec.strip()

    # Split type name and (optional) literal arguments
    if "(" in spec and spec.endswith(")"):
        name, args_str = spec.split("(", 1)
        args_str = args_str[:-1].strip()
    else:
        name, args_str = spec, ""
    name = name.strip()

    # Resolve the type class centrally (mapping lookup + dotted-path fallback)
    type_cls = resolve_type_class(name, mapping)

    # Parse positional args via ast.literal_eval (literals only, no code exec)
    if not args_str:
        return type_cls()
    try:
        parsed = ast.literal_eval(f"({args_str},)")
    except (ValueError, SyntaxError) as exc:
        raise ValueError(
            f"Invalid literal arguments for dtype '{name}': {args_str!r}"
        ) from exc

    return type_cls(*parsed)


# ============================================================================
# Engine factories
# ============================================================================


def create_sqlalchemy_engine(conn_dict: dict | None = None, **kwargs):
    """
    Create a SQLAlchemy engine from a connection dictionary.

    Parameters:
        conn_dict (dict): Dictionary with ``server``, ``database``, ``username``,
                          ``password``, ``port`` and ``engine_prefix`` keys.
                          A pre-built ``custom_conn_str`` can be supplied either
                          inside ``conn_dict`` or via ``kwargs``.
        **kwargs: Extra arguments forwarded to ``sqlalchemy.create_engine``.

    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine.
    """
    conn_dict = conn_dict or {}

    # Build default connection string
    default_custom_conn_str = "{}://{}:{}@{}:{}/{}".format(
        conn_dict.get("engine_prefix", "mssql+pyodbc"),
        conn_dict.get("username"),
        conn_dict.get("password"),
        conn_dict.get("server"),
        conn_dict.get("port", 1433),
        conn_dict.get("database"),
    )

    # Prefer explicit custom connection string (conn_dict > kwargs > default)
    custom_conn_str = conn_dict.get(
        "custom_conn_str",
        kwargs.get("custom_conn_str") or default_custom_conn_str,
    )

    # Strip keys that are not valid create_engine kwargs
    new_kwargs = {k: v for k, v in kwargs.items() if k != "custom_conn_str"}

    return create_engine(custom_conn_str, **new_kwargs)


def create_bigquery_engine(conn_dict: dict, **kwargs):
    """
    Create a BigQuery SQLAlchemy engine from a connection dictionary.

    Parameters:
        conn_dict (dict): Dictionary with at least ``database`` (the GCP
                          project/dataset reference).
        **kwargs: Extra arguments forwarded to ``create_sqlalchemy_engine``.
                  May include ``location`` and ``custom_conn_str``.

    Returns:
        sqlalchemy.engine.Engine: BigQuery engine.
    """
    # Location (conn_dict > kwargs > default)
    location = conn_dict.get(
        "location",
        kwargs.get("location") if kwargs.get("location") is not None else "us-east1",
    )

    # Build default connection string
    default_custom_conn_str = f"bigquery://{conn_dict.get('database')}"
    custom_conn_str = conn_dict.get(
        "custom_conn_str",
        kwargs.get("custom_conn_str") or default_custom_conn_str,
    )

    # Strip handled keys
    new_kwargs = {
        k: v for k, v in kwargs.items() if k not in ("location", "custom_conn_str")
    }

    return create_sqlalchemy_engine(
        conn_dict,
        custom_conn_str=custom_conn_str,
        location=location,
        **new_kwargs,
    )


def create_redshift_engine(conn_dict: dict, **kwargs):
    """
    Create a Redshift SQLAlchemy engine from a connection dictionary.

    Parameters:
        conn_dict (dict): Connection information including ``server``,
                          ``database``, ``username``, ``password`` and
                          optionally ``port`` and ``sslmode``.
        **kwargs: Extra arguments forwarded to ``create_sqlalchemy_engine``.

    Returns:
        sqlalchemy.engine.Engine: Redshift engine.
    """
    port = conn_dict.get(
        "port", kwargs.get("port") if kwargs.get("port") is not None else 5439
    )
    connect_args = (
        {"sslmode": conn_dict["sslmode"]}
        if "sslmode" in conn_dict
        else (kwargs.get("connect_args") or {"ssl_mode": "verify-ca"})
    )

    default_custom_conn_str = (
        f"redshift+redshift_connector://{conn_dict['username']}:"
        f"{conn_dict['password']}@{conn_dict['server']}:{port}/{conn_dict['database']}"
    )
    custom_conn_str = conn_dict.get(
        "custom_conn_str", kwargs.get("custom_conn_str") or default_custom_conn_str
    )

    new_kwargs = {
        k: v
        for k, v in kwargs.items()
        if k not in ("port", "connect_args", "custom_conn_str")
    }

    return create_sqlalchemy_engine(
        conn_dict,
        custom_conn_str=custom_conn_str,
        connect_args=connect_args,
        **new_kwargs,
    )


def _init_oracle_client(conn_dict: dict) -> None:
    """Best-effort initialisation of the Oracle client library.

    The original implementation swallowed all exceptions to allow Linux setups
    to fall back from the Windows path; we keep that semantic but route every
    message through the logger instead of ``print``.
    """
    try:
        try:  # Windows path
            logger.info(
                "Starting oracle client on Windows -> "
                f"{conn_dict['oracle_client_dir']}"
            )
            oracledb.init_oracle_client(lib_dir=conn_dict["oracle_client_dir"])
        except Exception as e_win:  # Linux fallback
            logger.warning(
                f"Error starting oracle client on Windows -> {type(e_win)} - {e_win}"
            )
            logger.info(
                "Starting oracle client on Linux -> "
                f"{conn_dict['oracle_client_dir']}"
            )
            os.environ["LD_LIBRARY_PATH"] = conn_dict["oracle_client_dir"]
            oracledb.init_oracle_client()
    except Exception as e:  # Already started or not needed
        logger.info(f"Oracle client init skipped -> {type(e)} - {e}")


def create_oracle_engine(conn_dict: dict, **kwargs):
    """
    Create an Oracle SQLAlchemy engine from a connection dictionary.

    Parameters:
        conn_dict (dict): Connection info including ``server``, ``database``,
                          ``username``, ``password`` and ``oracle_client_dir``.
        **kwargs: Extra arguments forwarded to ``create_sqlalchemy_engine``.

    Returns:
        sqlalchemy.engine.Engine: Oracle engine.
    """
    _init_oracle_client(conn_dict)

    port = conn_dict.get(
        "port", kwargs.get("port") if kwargs.get("port") is not None else 1521
    )

    default_custom_conn_str = (
        f"oracle+cx_oracle://{conn_dict['username']}:{conn_dict['password']}"
        f"@{conn_dict['server']}:{port}/?service_name={conn_dict['database']}"
    )
    custom_conn_str = conn_dict.get(
        "custom_conn_str", kwargs.get("custom_conn_str") or default_custom_conn_str
    )

    new_kwargs = {
        k: v for k, v in kwargs.items() if k not in ("port", "custom_conn_str")
    }

    return create_sqlalchemy_engine(
        conn_dict, custom_conn_str=custom_conn_str, **new_kwargs
    )


def create_mysql_engine(conn_dict: dict, **kwargs):
    """
    Create a MySQL SQLAlchemy engine from a connection dictionary.

    Parameters:
        conn_dict (dict): Connection info including ``server``, ``database``,
                          ``username``, ``password`` and optionally ``port``
                          and ``charset``.
        **kwargs: Extra arguments forwarded to ``create_sqlalchemy_engine``.

    Returns:
        sqlalchemy.engine.Engine: MySQL engine.
    """
    port = conn_dict.get(
        "port", kwargs.get("port") if kwargs.get("port") is not None else 3306
    )
    connect_args = (
        {"charset": conn_dict["charset"]}
        if "charset" in conn_dict
        else (kwargs.get("connect_args") or {"charset": "utf8mb4"})
    )

    default_custom_conn_str = (
        f"mysql+pymysql://{conn_dict['username']}:{conn_dict['password']}"
        f"@{conn_dict['server']}:{port}/{conn_dict['database']}"
    )
    custom_conn_str = conn_dict.get(
        "custom_conn_str", kwargs.get("custom_conn_str") or default_custom_conn_str
    )

    new_kwargs = {
        k: v
        for k, v in kwargs.items()
        if k not in ("port", "connect_args", "custom_conn_str")
    }

    return create_sqlalchemy_engine(
        conn_dict,
        custom_conn_str=custom_conn_str,
        connect_args=connect_args,
        **new_kwargs,
    )


def create_cloudsql_engine(conn_dict: dict, **kwargs):
    """
    Create a Cloud SQL SQLAlchemy engine from a connection dictionary.

    Parameters:
        conn_dict (dict): Cloud SQL instance connection information. Must
                          contain ``instance_connection_name``, ``database_type``,
                          ``database``, ``username``, ``password``.
        **kwargs: May include ``connector`` (``google.cloud.sql.connector.Connector``),
                  ``custom_conn_str`` and ``ip_type``.

    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine using Cloud SQL Connector
                                  when available, otherwise standard SQLAlchemy.
    """
    instance_connection_name = conn_dict.get("instance_connection_name")
    database_type = conn_dict.get("database_type", "mysql").lower()
    database = conn_dict.get("database")
    username = conn_dict.get("username")
    password = conn_dict.get("password")

    connector = kwargs.get("connector")
    custom_conn_str = kwargs.get("custom_conn_str")
    ip_type = kwargs.get("ip_type")

    # Normalise dialect names
    if database_type in ("postgres", "postgresql"):
        database_type = "postgresql"
    elif database_type in ("mysql", "sqlserver"):
        database_type = "mssql"
    driver_map = {
        "mysql": "pymysql",
        "postgresql": "pg8000",
        "mariadb": "pymysql",
        "mssql": "pytds",
    }
    driver = driver_map.get(database_type, database_type)

    # Prefer Cloud SQL Connector when supplied
    if connector is not None:
        logger.info(
            f"Creating Cloud SQL engine for {database_type} using Cloud SQL Connector..."
        )
        cloudsql_conn_str = f"{database_type}+{driver}:///{database}"

        try:
            def getconn():
                return connector.connect(
                    instance_connection_name,
                    driver,
                    user=username,
                    password=password,
                    db=database,
                    ip_type=ip_type,
                )

            return create_engine(cloudsql_conn_str, creator=getconn)
        except Exception as e:
            logger.error(
                f"Error creating Cloud SQL engine with connector -> {type(e)} - {e}. "
                "Falling back to SQLAlchemy..."
            )

    # Fallback: standard SQLAlchemy connection string
    logger.info("Using SQLAlchemy fallback for Cloud SQL connection...")
    if custom_conn_str is None:
        custom_conn_str = (
            f"{database_type}+{driver}://{username}:{password}@/"
            f"cloudsql_{instance_connection_name.replace(':', '_')}/{database}"
        )

    new_kwargs = {
        k: v for k, v in kwargs.items() if k not in ("connector", "custom_conn_str")
    }
    return create_sqlalchemy_engine(
        conn_dict, custom_conn_str=custom_conn_str, **new_kwargs
    )


# ============================================================================
# Connection factories
# ============================================================================


def create_cloudsql_conn(conn_dict: dict, **kwargs):
    """Open a Cloud SQL connection. See :func:`create_cloudsql_engine`."""
    engine = create_cloudsql_engine(conn_dict, **kwargs)
    logger.info("Connecting to Cloud SQL database...")
    return engine.connect()


def create_sqlalchemy_conn(conn_dict: dict, custom_conn_str=None, **kwargs):
    """Open a generic SQLAlchemy connection."""
    engine = create_sqlalchemy_engine(
        conn_dict, custom_conn_str=custom_conn_str, **kwargs
    )
    logger.info("Connecting to database...")
    return engine.connect()


def create_bigquery_conn(conn_dict: dict, **kwargs):
    """Open a BigQuery connection."""
    engine = create_bigquery_engine(conn_dict, **kwargs)
    logger.info("Connecting to database...")
    return engine.connect()


def create_redshift_conn(conn_dict: dict, **kwargs):
    """Open a Redshift connection using ``redshift_connector``."""
    conn_dict.setdefault("port", 5439)
    return redshift_connector.connect(
        host=conn_dict["server"],
        database=conn_dict["database"],
        port=conn_dict["port"],
        user=conn_dict["username"],
        password=conn_dict["password"],
        **kwargs,
    )


def create_oracle_conn(conn_dict: dict, **kwargs):
    """Open a raw Oracle connection using ``oracledb``."""
    _init_oracle_client(conn_dict)
    return oracledb.connect(
        user=conn_dict["username"],
        password=conn_dict["password"],
        dsn=f"{conn_dict['server']}/{conn_dict['database']}",
        **kwargs,
    )


def create_mysql_conn(conn_dict: dict, **kwargs):
    """Open a raw MySQL connection using ``pyodbc``."""
    conn_dict.setdefault("driver", "{MySQL ODBC 8.0 Unicode Driver}")
    conn_dict.setdefault("port", 3306)
    conn_dict.setdefault("charset", "utf8mb4")

    str_conn = (
        f"DRIVER={conn_dict['driver']};"
        f"SERVER={conn_dict['server']};"
        f"DATABASE={conn_dict['database']};"
        f"UID={conn_dict['username']};"
        f"PWD={conn_dict['password']};"
        f"PORT={conn_dict['port']};"
        f"CHARSET={conn_dict['charset']}"
    )
    return pyodbc.connect(str_conn, **kwargs)


def create_pyodbc_conn(conn_dict: dict, **kwargs):
    """Open a raw SQL Server connection using ``pyodbc``."""
    conn_dict.setdefault("driver", "{ODBC Driver 17 for SQL Server}")
    str_conn = (
        f"DRIVER={conn_dict['driver']};"
        f"SERVER={conn_dict['server']};"
        f"DATABASE={conn_dict['database']};"
        f"UID={conn_dict['username']};"
        f"PWD={conn_dict['password']}"
    )
    return pyodbc.connect(str_conn, **kwargs)


# ============================================================================
# Internal dispatch helpers
# ============================================================================

_CONN_FACTORIES = {
    "pyodbc": create_pyodbc_conn,
    "redshift": create_redshift_conn,
    "sqlalchemy": create_sqlalchemy_conn,
    "oracledb": create_oracle_conn,
    "bigquery": create_bigquery_conn,
    "cloudsql": create_cloudsql_conn,
    "mysql": create_mysql_conn,
}

_ENGINE_FACTORIES = {
    "pyodbc": create_sqlalchemy_engine,
    "sqlalchemy": create_sqlalchemy_engine,
    "redshift": create_redshift_engine,
    "oracledb": create_oracle_engine,
    "bigquery": create_bigquery_engine,
    "cloudsql": create_cloudsql_engine,
    "mysql": create_mysql_engine,
}


def _make_conn(mode: str, conn_dict: dict, **kwargs):
    mode_l = mode.lower()
    if mode_l not in _CONN_FACTORIES:
        raise ValueError(
            f"Invalid connection mode '{mode}'. "
            f"Allowed modes: {sorted(_CONN_FACTORIES)}"
        )
    logger.info(f"Connecting to database (mode={mode_l})...")
    return _CONN_FACTORIES[mode_l](conn_dict, **kwargs)


def _make_engine(mode: str, conn_dict: dict, **kwargs):
    mode_l = mode.lower()
    if mode_l not in _ENGINE_FACTORIES:
        raise ValueError(
            f"Invalid engine mode '{mode}'. "
            f"Allowed modes: {sorted(_ENGINE_FACTORIES)}"
        )
    return _ENGINE_FACTORIES[mode_l](conn_dict, **kwargs)


# ============================================================================
# Data manipulation
# ============================================================================


def to_sql_executemany(data, conn_dict, schema, table_name, mode, **kwargs):
    """
    Upload data to a database table using ``cursor.executemany``.

    Parameters:
        data (pd.DataFrame): Data to upload.
        conn_dict (dict): Connection info.
        schema (str): Schema name.
        table_name (str): Target table.
        mode (str): One of the keys in :data:`_CONN_FACTORIES`.
        **kwargs: Extra arguments forwarded to the connection factory.

    Returns:
        int: Number of rows affected.
    """
    sql_conn = _make_conn(mode, conn_dict, **kwargs)

    logger.info("Executing statement...")
    sql_stmt = (
        f"INSERT INTO {schema}.{table_name} "
        f"({','.join(col for col in data.columns)}) "
        f"VALUES ({','.join(f':{col}' for col in data.columns)})"
    )
    data_rows = [tuple(x) for x in data.to_numpy()]
    with sql_conn:
        cursor = sql_conn.cursor()
        cursor.executemany(sql_stmt, data_rows)
        response_rows_affected = cursor.rowcount
        sql_conn.commit()

    return response_rows_affected


def to_sql_redshift_spark(data, schema, table_name, conn_dict, mode="append", **kwargs):
    """
    Upload data to Redshift using Spark.

    Parameters:
        data (pd.DataFrame): Data to upload.
        schema (str): Schema name.
        table_name (str): Target table.
        conn_dict (dict): Connection info (uses ``database``, ``username``,
                          ``password``).
        mode (str): One of ``'append'``, ``'overwrite'``, ``'ignore'``,
                    ``'error'``.
        **kwargs: Currently unused; accepted for signature compatibility.

    Returns:
        int: Number of rows inserted.
    """
    spark_session = (
        ps.sql.SparkSession.builder.appName("UploadDataPipeline")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark_df = spark_session.createDataFrame(data)
    (
        spark_df.write.format("io.github.spark_redshift_community.spark.redshift")
        .option("url", f"jdbc:redshift://{conn_dict['database']}")
        .option("dbtable", f"{schema}.{table_name}")
        .option("user", conn_dict["username"])
        .option("password", conn_dict["password"])
        .option("tempdir", "s3://tmp-spark/")
        .mode(mode)
        .save()
    )
    return spark_df.count()


def parallel_to_sql(
    df,
    table_name,
    schema,
    mode,
    conn_dict,
    custom_conn_str,
    connect_args,
    chunksize,
    method,
    dtypes_dict,
    spark_mode="append",
    **kwargs,
):
    """
    Upload data to a database table using one of several strategies.

    Parameters:
        df (pd.DataFrame): Data to upload.
        table_name (str): Target table.
        schema (str): Schema name.
        mode (str): Engine mode (see :data:`_ENGINE_FACTORIES`).
        conn_dict (dict): Connection info.
        custom_conn_str (str | None): Optional custom connection string.
        connect_args (dict): Forwarded to the SQLAlchemy engine.
        chunksize (int): Pandas ``to_sql`` chunksize.
        method (str): ``'multi'``, ``'execute_many'``, ``'spark'`` or ``'single'``.
        dtypes_dict (dict): SQLAlchemy dtype dict for ``to_sql``.
        spark_mode (str): Mode for Spark Redshift writes.
        **kwargs: Extra arguments for connection factories.

    Returns:
        int: Rows affected.
    """
    logger.info("Connecting to database...")
    engine = _make_engine(
        mode, conn_dict, custom_conn_str=custom_conn_str,
        connect_args=connect_args, **kwargs,
    )

    logger.info("Uploading data...")

    method_l = method.lower()
    if method_l == "multi":
        try:
            logger.info("Trying to upload data with 'multi' method...")
            return df.to_sql(
                table_name,
                engine,
                schema=schema,
                if_exists="append",
                index=False,
                chunksize=chunksize,
                method=method,
                dtype=dtypes_dict,
            )
        except Exception as e:
            logger.warning(f"{type(e)} - {e}")
            try:
                logger.info(
                    "Multi upload failed. Falling back to 'execute_many' method..."
                )
                return to_sql_executemany(
                    df, conn_dict, schema, table_name, mode, **kwargs
                )
            except Exception as e2:
                logger.warning(f"{type(e2)} - {e2}")
                logger.info(
                    "Execute-many upload failed. Falling back to 'single' method..."
                )
                return df.to_sql(
                    table_name,
                    engine,
                    schema=schema,
                    if_exists="append",
                    index=False,
                    chunksize=chunksize,
                    dtype=dtypes_dict,
                )
    elif method_l == "execute_many":
        try:
            logger.info("Trying to upload data with 'execute_many' method...")
            return to_sql_executemany(
                df, conn_dict, schema, table_name, mode, **kwargs
            )
        except Exception as e:
            logger.warning(f"{type(e)} - {e}")
            logger.info(
                "Execute-many upload failed. Falling back to 'single' method..."
            )
            return df.to_sql(
                table_name,
                engine,
                schema=schema,
                if_exists="append",
                index=False,
                chunksize=chunksize,
                dtype=dtypes_dict,
            )
    elif method_l == "spark":
        try:
            logger.info("Trying to upload data with 'spark' method...")
            return to_sql_redshift_spark(
                df, schema, table_name, conn_dict, mode=spark_mode
            )
        except Exception as e:
            logger.warning(f"{type(e)} - {e}")
            logger.info("Spark upload failed. Falling back to 'single' method...")
            return df.to_sql(
                table_name,
                engine,
                schema=schema,
                if_exists="append",
                index=False,
                chunksize=chunksize,
                dtype=dtypes_dict,
            )
    elif method_l == "single":
        logger.info("Uploading data with 'single' method...")
        return df.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists="append",
            index=False,
            chunksize=chunksize,
            dtype=dtypes_dict,
        )
    else:
        logger.error(f"Unknown upload method '{method}'. Aborting...")
        return 0


def sql_exec_stmt(sql_stmt, conn_dict: dict, mode="pyodbc", **kwargs):
    """
    Execute a SQL statement and return ``(rows_affected, output)``.

    Parameters:
        sql_stmt (str | sqlalchemy.schema.DDL): Statement to execute.
        conn_dict (dict): Connection info.
        mode (str): Connection mode.
        **kwargs: Extra arguments forwarded to the connection factory.

    Returns:
        tuple[int, Any | None]: Rows affected and fetched output when
                                available, otherwise ``None``.
    """
    sql_conn = _make_conn(mode, conn_dict, **kwargs)

    logger.info("Executing statement...")
    response_output = None
    response_rows_affected = 0
    with sql_conn:
        try:
            cursor = sql_conn.cursor()
            cursor.execute(sql_stmt)
            response_rows_affected = cursor.rowcount
            try:
                response_output = cursor.fetchall()
            except Exception as e:
                logger.info(f"No results to fetch - {type(e)}: {e}")
                response_output = None
            sql_conn.commit()
        except Exception as e:
            logger.error(
                f"Error executing statement -> {type(e)} - {e}. "
                "Retrying without cursor..."
            )
            try:
                result = sql_conn.execute(sql_stmt)
                response_rows_affected = 1
                try:
                    response_output = result.fetchall()
                except Exception as e_fetch:
                    logger.info(
                        f"No results to fetch - {type(e_fetch)}: {e_fetch}"
                    )
                    response_output = None
                sql_conn.commit()
            except Exception as e_retry:
                logger.error(
                    f"Error executing statement -> {type(e_retry)} - {e_retry}. "
                    "Aborting..."
                )
                response_rows_affected = 0
                # Re-raise the original failure so callers can react instead of
                # silently receiving zero rows. (Previously the error was only
                # printed and the function returned ``(0, None)``.)
                raise

    return response_rows_affected, response_output


# ============================================================================
# High-level read / upload / copy helpers
# ============================================================================


def _log_exception(log_file_path: str, log_file_name: str, name: str) -> None:
    """Persist current exception to both summary and detailed logs."""
    os.makedirs(log_file_path, exist_ok=True)
    exc_info = sys.exc_info()
    summary = traceback.format_exception(*exc_info)[-1]
    detailed = "".join(traceback.format_exception(*exc_info))
    caller = sys._getframe(1).f_code.co_name + " -> " + (name or "")
    mk_err_logs(log_file_path, log_file_name, caller, summary, mode="summary")
    mk_err_logs(log_file_path, log_file_name, caller, detailed, mode="detailed")


def sql_read_data(
    sql_stmt,
    conn_dict,
    custom_conn_str=None,
    connect_args=None,
    mode="sqlalchemy",
    name=None,
    max_n_try=3,
    log_file_path="logs",
    **kwargs,
):
    """
    Read data using a SQL statement, with retries and error logging.

    Parameters:
        sql_stmt: SQL statement.
        conn_dict (dict): Connection info.
        custom_conn_str (str | None): Optional custom connection string.
        connect_args (dict | None): Forwarded to the SQLAlchemy engine.
        mode (str): Engine mode.
        name (str | None): Name used for log messages.
        max_n_try (int): Maximum number of retries.
        log_file_path (str): Directory for error/timing logs.
        **kwargs: Extra arguments forwarded to the engine factory.

    Returns:
        pd.DataFrame: Query results (empty on failure after retries).
    """
    if connect_args is None:
        connect_args = {}

    df = pd.DataFrame()
    t_i = dt.datetime.now()
    n_try = 0
    succeeded = False
    last_exc: Exception | None = None
    while n_try < max_n_try and not succeeded:
        try:
            engine_obj = _make_engine(
                mode, conn_dict,
                custom_conn_str=custom_conn_str,
                connect_args=connect_args,
                **kwargs,
            )
            df = pd.read_sql(sql_stmt, engine_obj)
            engine_obj.dispose()
            succeeded = True
        except Exception as e:
            last_exc = e
            df = pd.DataFrame()
            _log_exception(log_file_path, "read_data", name or "")
            logger.error(
                f"sql_read_data attempt {n_try + 1}/{max_n_try} failed "
                f"(name={name}) -> {type(e).__name__}: {e}"
            )
            succeeded = False
        n_try += 1

    t_e = dt.datetime.now()
    logger.info(
        f"Time elapsed in 'download' process {name} -> {df.shape} = {t_e - t_i}"
    )

    os.makedirs(log_file_path, exist_ok=True)
    mk_texec_logs(
        log_file_path,
        "download_data_texec",
        sys._getframe().f_code.co_name + " -> " + (name or ""),
        t_e - t_i,
        obs=f"Shape of object = {df.shape}",
    )

    if not succeeded and last_exc is not None:
        logger.error(
            f"sql_read_data exhausted retries for '{name}'. "
            f"Last error: {type(last_exc).__name__}: {last_exc}"
        )
        raise last_exc

    return df


def sql_upload_data(
    df,
    schema,
    table_name,
    conn_dict,
    custom_conn_str=None,
    mode="sqlalchemy",
    connect_args=None,
    name=None,
    chunksize=1000,
    method="multi",
    max_n_try=3,
    dtypes_dict=None,
    n_jobs=-1,
    spark_mode="append",
    log_file_path="logs",
    **kwargs,
):
    """
    Upload a DataFrame to a SQL table, with retries and error logging.

    Parameters:
        df (pd.DataFrame): Data to upload.
        schema (str): Target schema.
        table_name (str): Target table.
        conn_dict (dict): Connection info.
        custom_conn_str (str | None): Optional custom connection string.
        mode (str): Engine mode.
        connect_args (dict | None): Forwarded to the SQLAlchemy engine.
        name (str | None): Name used for log messages.
        chunksize (int): Pandas ``to_sql`` chunksize.
        method (str): Upload strategy (see :func:`parallel_to_sql`).
        max_n_try (int): Maximum number of retries.
        dtypes_dict (dict | None): SQLAlchemy dtype dict for ``to_sql``.
        n_jobs (int): Parallelism (``-1`` = all CPUs).
        spark_mode (str): Spark write mode.
        log_file_path (str): Directory for error/timing logs.
        **kwargs: Extra arguments forwarded to the engine factory.

    Returns:
        int: Number of rows affected (``0`` on failure).
    """
    if connect_args is None:
        connect_args = {}
    if dtypes_dict is None:
        dtypes_dict = {}

    if n_jobs == -1:
        n_jobs = multiprocessing.cpu_count()

    # Create schema if not exists
    try:
        rows_affected_schema, _ = sql_exec_stmt(
            DDL(f"CREATE SCHEMA IF NOT EXISTS {schema}"),
            conn_dict,
            mode=mode,
            **kwargs,
        )
        logger.info(f"Schema {schema} created -> {rows_affected_schema}")
    except Exception as e:
        logger.warning(f"Error creating schema {schema} -> {type(e)} - {e}")

    response_rows_affected = 0
    t_i = dt.datetime.now()
    n_try = 0
    succeeded = False
    while n_try < max_n_try and not succeeded:
        try:
            logger.info(f"Shape of query dataframe -> {name} = {df.shape}")
            if not df.empty:
                if df.shape[0] / chunksize >= n_jobs:
                    logger.info("Uploading chunked data in parallel...")
                    df_split_iter = [
                        x for x in np.array_split(df, n_jobs) if not x.empty
                    ]
                    table_name_iter = [table_name] * len(df_split_iter)
                    schema_iter = [schema] * len(df_split_iter)
                    mode_iter = [mode] * len(df_split_iter)
                    conn_dict_iter = [conn_dict] * len(df_split_iter)
                    custom_conn_str_iter = [custom_conn_str] * len(df_split_iter)
                    connect_args_iter = [connect_args] * len(df_split_iter)
                    chunksize_iter = [chunksize] * len(df_split_iter)
                    method_iter = [method] * len(df_split_iter)
                    dtypes_iter = [dtypes_dict] * len(df_split_iter)
                    spark_mode_iter = [spark_mode] * len(df_split_iter)
                    kwargs_iter = [kwargs] * len(df_split_iter)
                    parallel_results = parallel_execute(
                        parallel_to_sql,
                        df_split_iter,
                        table_name_iter,
                        schema_iter,
                        mode_iter,
                        conn_dict_iter,
                        custom_conn_str_iter,
                        connect_args_iter,
                        chunksize_iter,
                        method_iter,
                        dtypes_iter,
                        spark_mode_iter,
                        kwargs_iter,
                    )
                    response_rows_affected = sum(parallel_results or [])
                else:
                    logger.info("Uploading whole data...")
                    response_rows_affected = parallel_to_sql(
                        df,
                        table_name,
                        schema,
                        mode,
                        conn_dict,
                        custom_conn_str,
                        connect_args,
                        chunksize,
                        method,
                        dtypes_dict,
                        spark_mode,
                        **kwargs,
                    )
            else:
                response_rows_affected = 0
            logger.info(
                f"Affected number of rows -> {name} = {response_rows_affected}"
            )
            succeeded = True
        except Exception as e:
            _log_exception(log_file_path, "upload_data", name or "")
            logger.error(
                f"sql_upload_data attempt {n_try + 1}/{max_n_try} failed "
                f"(name={name}) -> {type(e).__name__}: {e}"
            )
            response_rows_affected = 0
            succeeded = False
        n_try += 1

    t_e = dt.datetime.now()
    logger.info(
        f"Time elapsed in 'upload' process {name} -> {df.shape} = {t_e - t_i}"
    )

    os.makedirs(log_file_path, exist_ok=True)
    mk_texec_logs(
        log_file_path,
        "upload_data_texec",
        sys._getframe().f_code.co_name + " -> " + (name or ""),
        t_e - t_i,
        obs=f"Shape of object = {df.shape}",
    )

    if not succeeded:
        raise RuntimeError(
            f"sql_upload_data failed after {max_n_try} attempts for '{name}'. "
            "Check logs for details."
        )

    return response_rows_affected


def sql_copy_data(
    s3_file_path,
    schema,
    table_name,
    conn_dict,
    access_key,
    secret_access_key,
    region,
    delimiter=",",
    header_row=1,
    type_format="csv",
    name=None,
    max_n_try=3,
    log_file_path="logs",
    **kwargs,
):
    """
    Copy data from an S3 bucket into a Redshift table using ``COPY``.

    .. warning::
        The Redshift ``COPY`` statement is interpolated with the supplied
        ``access_key``, ``secret_access_key``, ``region`` and ``s3_file_path``.
        Callers must validate/sanitize these values to avoid SQL injection
        through credential or path values that originate from untrusted input.

    Parameters:
        s3_file_path (str): S3 file path(s) to copy from.
        schema (str): Target schema.
        table_name (str): Target table.
        conn_dict (dict): Redshift connection info.
        access_key (str): S3 access key.
        secret_access_key (str): S3 secret access key.
        region (str): AWS region.
        delimiter (str): Field delimiter (default ``','``).
        header_row (int): Number of header rows to ignore (default ``1``).
        type_format (str): Source format (default ``'csv'``).
        name (str | None): Name used for log messages.
        max_n_try (int): Maximum number of retries.
        log_file_path (str): Directory for error/timing logs.
        **kwargs: Extra arguments forwarded to the connection factory.

    Returns:
        int: Rows affected.
    """
    response_rows_affected = 0
    t_i = dt.datetime.now()
    n_try = 0
    succeeded = False
    while n_try < max_n_try and not succeeded:
        try:
            sql_stmt = (
                f"COPY {schema}.{table_name} FROM '{s3_file_path}' "
                f"ACCESS_KEY_ID '{access_key}' "
                f"SECRET_ACCESS_KEY '{secret_access_key}' "
                f"REGION '{region}' DELIMITER '{delimiter}' "
                f"IGNOREHEADER {header_row} EMPTYASNULL "
                f"FORMAT AS {type_format.upper()};"
            )
            response_rows_affected, _ = sql_exec_stmt(
                sql_stmt, conn_dict, mode="redshift", **kwargs
            )
            logger.info(
                f"Affected number of rows -> {name} = {response_rows_affected}"
            )
            succeeded = True
        except Exception as e:
            _log_exception(log_file_path, "copy_data", name or "")
            logger.error(
                f"sql_copy_data attempt {n_try + 1}/{max_n_try} failed "
                f"(name={name}) -> {type(e).__name__}: {e}"
            )
            response_rows_affected = 0
            succeeded = False
        n_try += 1

    t_e = dt.datetime.now()
    logger.info(f"Time elapsed in 'copy' process {name} = {t_e - t_i}")

    os.makedirs(log_file_path, exist_ok=True)
    mk_texec_logs(
        log_file_path,
        "copy_data_texec",
        sys._getframe().f_code.co_name + " -> " + (name or ""),
        t_e - t_i,
        obs="Copy data from S3 bucket to database table",
    )

    if not succeeded:
        raise RuntimeError(
            f"sql_copy_data failed after {max_n_try} attempts for '{name}'. "
            "Check logs for details."
        )

    return response_rows_affected
