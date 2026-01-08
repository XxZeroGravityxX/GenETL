# Import modules
import os
import sys
import traceback
import pyspark as ps
import pyodbc
import redshift_connector
import oracledb
import numpy as np
import pandas as pd
import datetime as dt
import multiprocessing

# Import submodules
from sqlalchemy import create_engine
from sqlalchemy.schema import DDL

# Import custom modules
from etl_tools.execution import mk_err_logs, mk_texec_logs, parallel_execute


def create_sqlalchemy_engine(
    conn_dict: dict | None = None,
    **kwargs,
):
    """
    Function to create sqlalchemy engine from connection dictionary.

    Parameters:
        conn_dict: Dictionary with server, database, uid and pwd information.
                   Must contain a custom connection string "custom_conn_str", or,
                   "engine_prefix", "username", "password", "server", "port" and "database" to create it.
        **kwargs: Extra arguments for connection.

    Returns:
        engine: Sqlalchemy engine.
    """

    # Create custom connection string

    ## Default connection string
    default_custom_conn_str = "{}://{}:{}@{}:{}/{}".format(
        (
            conn_dict["engine_prefix"]
            if "engine_prefix" in conn_dict.keys()
            else "mssql+pyodbc"
        ),
        conn_dict["username"] if "username" in conn_dict.keys() else None,
        conn_dict["password"] if "password" in conn_dict.keys() else None,
        conn_dict["server"] if "server" in conn_dict.keys() else None,
        conn_dict["port"] if "port" in conn_dict.keys() else 1433,
        conn_dict["database"] if "database" in conn_dict.keys() else None,
    )
    ## Custom connection string
    custom_conn_str = (
        conn_dict["custom_conn_str"]
        if "custom_conn_str" in conn_dict.keys()
        else (
            kwargs["custom_conn_str"]
            if (
                "custom_conn_str" in kwargs.keys()
                and kwargs["custom_conn_str"] is not None
            )
            else default_custom_conn_str
        )
    )

    # Remove keys from kwargs
    new_kwargs = {k: v for k, v in kwargs.items() if k not in ["custom_conn_str"]}

    ## Create engine
    engine = create_engine(
        custom_conn_str,
        **new_kwargs,
    )

    return engine


def create_bigquery_engine(conn_dict: dict, **kwargs):
    """
    Function to create bigquery engine from connection dictionary.

    Parameters:
        conn_dict: Dictionary with server, database, uid and pwd information.
        **kwargs: Extra arguments for connection.

    Returns:
        engine: Bigquery engine.
    """

    # Set extra parameters for connection

    ## Location
    location = (
        conn_dict["location"]
        if "location" in conn_dict.keys()
        else (
            kwargs["location"]
            if ("location" in kwargs.keys() and kwargs["location"] is not None)
            else "us-east1"
        )
    )
    ## Create custom connection string

    ### Default connection string
    default_custom_conn_str = "{}://{}".format(
        "bigquery",
        conn_dict["database"],
    )
    ### Custom connection string
    custom_conn_str = (
        conn_dict["custom_conn_str"]
        if "custom_conn_str" in conn_dict.keys()
        else (
            kwargs["custom_conn_str"]
            if (
                "custom_conn_str" in kwargs.keys()
                and kwargs["custom_conn_str"] is not None
            )
            else default_custom_conn_str
        )
    )

    # Remove keys from kwargs
    new_kwargs = {
        k: v for k, v in kwargs.items() if k not in ["location", "custom_conn_str"]
    }
    # Create engine
    engine = create_sqlalchemy_engine(
        conn_dict,
        custom_conn_str=custom_conn_str,
        location=location,
        **new_kwargs,
    )

    return engine


def create_redshift_engine(conn_dict: dict, **kwargs):
    """
    Function to create redshift engine from connection dictionary.

    Parameters:
        conn_dict: Dictionary with server, database, uid and pwd information.
        **kwargs: Extra arguments for connection.

    Returns:
        engine: Redshift engine.
    """

    # Set extra configuration for connection

    ## Port
    port = (
        conn_dict["port"]
        if "port" in conn_dict.keys()
        else (
            kwargs["port"]
            if ("port" in kwargs.keys() and kwargs["port"] is not None)
            else 5439
        )
    )
    ## Connect args
    connect_args = (
        {"sslmode": conn_dict["sslmode"]}
        if "sslmode" in conn_dict.keys()
        else (
            kwargs["connect_args"]
            if ("connect_args" in kwargs.keys() and kwargs["connect_args"] is not {})
            else {"ssl_mode": "verify-ca"}
        )
    )

    ## Create custom connection string

    ### Default connection string
    default_custom_conn_str = "{}://{}:{}@{}:{}/{}".format(
        "redshift+redshift_connector",
        conn_dict["username"],
        conn_dict["password"],
        conn_dict["server"],
        port,
        conn_dict["database"],
    )
    ### Custom connection string
    custom_conn_str = (
        conn_dict["custom_conn_str"]
        if "custom_conn_str" in conn_dict.keys()
        else (
            kwargs["custom_conn_str"]
            if (
                "custom_conn_str" in kwargs.keys()
                and kwargs["custom_conn_str"] is not None
            )
            else default_custom_conn_str
        )
    )

    # Remove keys from kwargs
    new_kwargs = {
        k: v
        for k, v in kwargs.items()
        if k not in ["port", "connect_args", "custom_conn_str"]
    }
    # Create engine
    engine = create_sqlalchemy_engine(
        conn_dict,
        custom_conn_str=custom_conn_str,
        connect_args=connect_args,
        **new_kwargs,
    )

    return engine


def create_oracle_engine(conn_dict: dict, **kwargs):
    """
    Function to create oracle engine from connection dictionary.

    Parameters:
        conn_dict: Dictionary with server, database, uid and pwd information.
        **kwargs: Extra arguments for connection.

    Returns:
        engine: Oracle engine.
    """

    # Start oracle client
    try:
        try:  # Windows
            print(
                f"Starting oracle client on Windows -> {conn_dict['oracle_client_dir']}"
            )
            ## Start oracle client
            oracledb.init_oracle_client(lib_dir=conn_dict["oracle_client_dir"])
        except Exception as e:  # Linux
            print(f"Error starting oracle client on Windows -> {type(e)} - {e}")
            print(
                f"Starting oracle client on Linux -> {conn_dict['oracle_client_dir']}"
            )
            ## Set environment variable
            os.environ["LD_LIBRARY_PATH"] = conn_dict["oracle_client_dir"]
            ## Start oracle client
            oracledb.init_oracle_client()
    except Exception as e:  # Oracle client already started or not needed
        print(f"Error starting oracle client -> {type(e)} - {e}")
        pass

    # Set extra configuration for connection

    ## Port
    port = (
        conn_dict["port"]
        if "port" in conn_dict.keys()
        else (
            kwargs["port"]
            if ("port" in kwargs.keys() and kwargs["port"] is not None)
            else 1521
        )
    )

    ## Create custom connection string

    ### Default connection string
    default_custom_conn_str = "{}://{}:{}@{}:{}/?service_name={}".format(
        "oracle+cx_oracle",
        conn_dict["username"],
        conn_dict["password"],
        conn_dict["server"],
        port,
        conn_dict["database"],
    )
    ### Custom connection string
    custom_conn_str = (
        conn_dict["custom_conn_str"]
        if "custom_conn_str" in conn_dict.keys()
        else (
            kwargs["custom_conn_str"]
            if (
                "custom_conn_str" in kwargs.keys()
                and kwargs["custom_conn_str"] is not None
            )
            else default_custom_conn_str
        )
    )

    # Remove keys from kwargs
    new_kwargs = {
        k: v for k, v in kwargs.items() if k not in ["port", "custom_conn_str"]
    }
    # Create engine
    engine = create_sqlalchemy_engine(
        conn_dict, custom_conn_str=custom_conn_str, **new_kwargs
    )

    return engine


def create_mysql_engine(conn_dict: dict, **kwargs):
    """
    Function to create mysql engine from connection dictionary.

    Parameters:
        conn_dict: Dictionary with server, database, uid and pwd information.
        **kwargs: Extra arguments for connection.

    Returns:
        engine: Mysql engine.
    """

    # Set extra configuration for connection

    ## Port
    port = (
        conn_dict["port"]
        if "port" in conn_dict.keys()
        else (
            kwargs["port"]
            if ("port" in kwargs.keys() and kwargs["port"] is not None)
            else 3306
        )
    )
    ## Connect args
    connect_args = (
        {"charset": conn_dict["charset"]}
        if "charset" in conn_dict.keys()
        else (
            kwargs["connect_args"]
            if ("connect_args" in kwargs.keys() and kwargs["connect_args"] is not {})
            else {"charset": "utf8mb4"}
        )
    )

    ## Create custom connection string

    ### Default connection string
    default_custom_conn_str = "{}://{}:{}@{}:{}/{}".format(
        "mysql+pymysql",
        conn_dict["username"],
        conn_dict["password"],
        conn_dict["server"],
        port,
        conn_dict["database"],
    )
    ### Custom connection string
    custom_conn_str = (
        conn_dict["custom_conn_str"]
        if "custom_conn_str" in conn_dict.keys()
        else (
            kwargs["custom_conn_str"]
            if (
                "custom_conn_str" in kwargs.keys()
                and kwargs["custom_conn_str"] is not None
            )
            else default_custom_conn_str
        )
    )

    # Remove keys from kwargs
    new_kwargs = {
        k: v
        for k, v in kwargs.items()
        if k not in ["port", "connect_args", "custom_conn_str"]
    }

    # Create engine
    engine = create_sqlalchemy_engine(
        conn_dict,
        custom_conn_str=custom_conn_str,
        connect_args=connect_args,
        **new_kwargs,
    )

    return engine


def create_sqlalchemy_conn(conn_dict: dict, custom_conn_str=None, **kwargs):
    """
    Function to create sqlalchemy connector from connection dictionary.

    Parameters:
        conn_dict: Dictionary with server, database, uid and pwd information.
        custom_conn_str: String with custom connection string.
        **kwargs: Extra arguments for connection.

    Returns:
        conn: Sqlalchemy connector.
    """
    # Create engine
    engine = create_sqlalchemy_engine(
        conn_dict, custom_conn_str=custom_conn_str, **kwargs
    )

    print("Connecting to database...")
    # Make connection
    conn = engine.connect()

    return conn


def create_bigquery_conn(conn_dict: dict, **kwargs):
    """
    Function to create bigquery connector from connection dictionary.

    Parameters:
        conn_dict: Dictionary with server, database, uid and pwd information.
        **kwargs: Extra arguments for connection.

    Returns:
        conn: Bigquery connector.
    """
    # Create engine
    engine = create_bigquery_engine(conn_dict, **kwargs)

    print("Connecting to database...")
    # Make connection
    conn = engine.connect()

    return conn


def create_redshift_conn(conn_dict: dict, **kwargs):
    """
    Function to create redshift connector from connection dictionary.

    Parameters:
        conn_dict: Dictionary with server, database, uid and pwd information.
        **kwargs: Extra arguments for connection.

    Returns:
        conn: Redshift connector.
    """

    # Set extra configuration for connection

    # Port
    if "port" not in conn_dict.keys():
        conn_dict["port"] = 5439

    # Create connector

    conn = redshift_connector.connect(
        host=conn_dict["server"],
        database=conn_dict["database"],
        port=conn_dict["port"],
        user=conn_dict["username"],
        password=conn_dict["password"],
        **kwargs,
    )

    return conn


def create_oracle_conn(conn_dict: dict, **kwargs):
    """
    Function to create oracle connector from connection dictionary.

    Parameters:
        conn_dict: Dictionary with server, database, uid and pwd information.
        **kwargs: Extra arguments for connection.

    Returns:
        conn: Oracle connector.
    """

    # Start oracle client
    try:
        try:  # Windows
            print(
                f"Starting oracle client on Windows -> {conn_dict['oracle_client_dir']}"
            )
            ## Start oracle client
            oracledb.init_oracle_client(lib_dir=conn_dict["oracle_client_dir"])
        except Exception as e:  # Linux
            print(f"Error starting oracle client on Windows -> {type(e)} - {e}")
            print(
                f"Starting oracle client on Linux -> {conn_dict['oracle_client_dir']}"
            )
            ## Set environment variable
            os.environ["LD_LIBRARY_PATH"] = conn_dict["oracle_client_dir"]
            ## Start oracle client
            oracledb.init_oracle_client()
    except Exception as e:  # Oracle client already started or not needed
        print(f"Error starting oracle client -> {type(e)} - {e}")
        pass

    # Create connector
    conn = oracledb.connect(
        user=conn_dict["username"],
        password=conn_dict["password"],
        dsn=f'{conn_dict["server"]}/{conn_dict["database"]}',
        **kwargs,
    )

    return conn


def create_mysql_conn(conn_dict: dict, **kwargs):
    """
    Function to create mysql connector from connection dictionary.

    Parameters:
        conn_dict: Dictionary with server, database, uid and pwd information.
        **kwargs: Extra arguments for connection.

    Returns:
        conn: Mysql connector.
    """

    # Set extra configuration for connection

    ## Driver
    if "driver" not in conn_dict.keys():
        conn_dict["driver"] = "{MySQL ODBC 8.0 Unicode Driver}"  # '{MySQL}'
    ## Port
    if "port" not in conn_dict.keys():
        conn_dict["port"] = 3306
    ## Charset
    if "charset" not in conn_dict.keys():
        conn_dict["charset"] = "utf8mb4"

    # Create string for connection

    str_conn = (
        f'DRIVER={conn_dict["driver"]};'
        + f'SERVER={conn_dict["server"]};'
        + f'DATABASE={conn_dict["database"]};'
        + f'UID={conn_dict["username"]};'
        + f'PWD={conn_dict["password"]}'
        + f'PORT={conn_dict["port"]};'
        + f'CHARSET={conn_dict["charset"]}'
    )
    # Create connector

    conn = pyodbc.connect(str_conn, **kwargs)

    return conn


def create_pyodbc_conn(conn_dict: dict, **kwargs):
    """
    Function to create pyodbc connector from connection dictionary.

    Parameters:
        conn_dict: Dictionary with server, database, uid and pwd information.
        **kwargs: Extra arguments for connection.

    Returns:
        conn: Pyodbc connector.
    """

    # Set extra configuration for connection

    ## Driver
    if "driver" not in conn_dict.keys():
        conn_dict["driver"] = "{ODBC Driver 17 for SQL Server}"  # '{SQL Server}'

    # Create string for connection
    str_conn = (
        f'DRIVER={conn_dict["driver"]};'
        + f'SERVER={conn_dict["server"]};'
        + f'DATABASE={conn_dict["database"]};'
        + f'UID={conn_dict["username"]};'
        + f'PWD={conn_dict["password"]}'
    )
    # Create connector
    conn = pyodbc.connect(str_conn, **kwargs)

    return conn


def to_sql_executemany(data, conn_dict, schema, table_name, mode, **kwargs):
    """
    Function to upload data to database table with sqlalchemy in parallel.

    Parameters:
        data: Pandas dataframe with data to upload.
        conn_dict: Dictionary with server, database, uid and pwd information.
        schema: String with schema name.
        table_name: String with table name to upload data.
        mode: String with mode to use. Options are 'pyodbc', 'redshift', 'sqlalchemy', 'oracledb', 'bigquery' and 'mysql'.
        **kwargs: Extra arguments for connection.

    Returns:
        response_rows_affected: Integer with number of rows inserted (should be equal to len(data)).
    """

    # Set mode of connection

    if mode == "pyodbc":
        print("Connecting to database...")
        ## Make connection
        sql_conn = create_pyodbc_conn(conn_dict, **kwargs)

    elif mode == "redshift":
        print("Connecting to database...")
        ## Make connection
        sql_conn = create_redshift_conn(conn_dict, **kwargs)

    elif mode == "sqlalchemy":
        print("Connecting to database...")
        ## Make connection
        sql_conn = create_sqlalchemy_conn(conn_dict, **kwargs)

    elif mode == "oracledb":
        print("Connecting to database...")
        ## Make connection
        sql_conn = create_oracle_conn(conn_dict, **kwargs)
    elif mode == "bigquery":
        print("Connecting to database...")
        ## Make connection
        sql_conn = create_bigquery_conn(conn_dict, **kwargs)
    elif mode == "mysql":
        print("Connecting to database...")
        ## Make connection
        sql_conn = create_mysql_conn(conn_dict, **kwargs)

    print("Executing statement...")
    # Execute statment

    ## Create sql statement
    sql_stmt = f"INSERT INTO {schema}.{table_name} ({','.join([col for col in data.columns])}) VALUES ({','.join([f':{col}' for col in data.columns])})"
    ## Format data into sequence
    data = [tuple(x) for x in data.to_numpy()]
    ## Make query
    with sql_conn:
        ### Initialize cursor
        cursor = sql_conn.cursor()
        ### Execute statement
        cursor.executemany(sql_stmt, data)
        ### Get number of rows affected
        response_rows_affected = cursor.rowcount
        ### Commit changes
        sql_conn.commit()

    return response_rows_affected  # Response with number of rows inserted (should be equal to len(data))


def to_sql_redshift_spark(data, schema, table_name, conn_dict, mode="append", **kwargs):
    """
    Function to upload data to redshift with spark.

    Parameters:
        data: Pandas dataframe with data to upload.
        schema: String with schema name.
        table_name: String with table name to upload data.
        conn_dict: Dictionary with server, database, uid and pwd information.
        mode: String with mode to use. Options are 'append', 'overwrite', 'ignore' and 'error'.
        **kwargs: Extra arguments for connection.

    Returns:
        response_rows_affected: Integer with number of rows inserted.
    """

    # Create spark session
    spark_session = (
        ps.sql.SparkSession.builder.appName("UploadDataPipeline")
        .enableHiveSupport()
        .getOrCreate()
    )
    # Create spark dataframe
    spark_df = spark_session.createDataFrame(data)
    # Upload data to redshift (options configuration from https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option)
    spark_df.write.format("io.github.spark_redshift_community.spark.redshift").option(
        "url",
        f'jdbc:redshift://{conn_dict["database"]}',
    ).option("dbtable", f"{schema}.{table_name}").option(
        "user", conn_dict["username"]
    ).option(
        "password", conn_dict["password"]
    ).option(
        "tempdir", "s3://tmp-spark/"
    ).mode(
        mode
    ).save()
    # Get response with number of rows inserted
    response_rows_affected = spark_df.count()

    return response_rows_affected


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
    Function to upload data to database table with sqlalchemy in parallel.

    Parameters:
        df: Pandas dataframe with data to upload.
        table_name: String with table name to upload data.
        schema: String with schema name.
        mode: String with mode to use. Options are 'pyodbc', 'redshift', 'sqlalchemy', 'oracledb', 'bigquery' and 'mysql'.
        conn_dict: Dictionary with server, database, uid and pwd information.
        custom_conn_str: String with custom connection string.
        connect_args: Dictionary with extra arguments for connection.
        chunksize: Integer with chunksize to use.
        method: String with method to use ('multi', 'execute_many', 'spark' or 'single').
        dtypes_dict: Dictionary with dtypes to use for upload.
        spark_mode: String with mode to use when uploading to redshift with spark. Options are 'append', 'overwrite', 'ignore' and 'error'.
        **kwargs: Extra arguments for connection.

    Returns:
        tot_response_rows_affected: Integer with total number of rows inserted.
    """

    print("Connecting to database...")
    # Create engine connection

    if mode.lower() == "pyodbc":
        engine = create_sqlalchemy_engine(
            conn_dict,
            custom_conn_str=custom_conn_str,
            connect_args=connect_args,
            **kwargs,
        )
    elif mode.lower() == "redshift":
        engine = create_redshift_engine(
            conn_dict,
            custom_conn_str=custom_conn_str,
            connect_args=connect_args,
            **kwargs,
        )
    elif mode.lower() == "sqlalchemy":
        engine = create_sqlalchemy_engine(
            conn_dict,
            custom_conn_str=custom_conn_str,
            connect_args=connect_args,
            **kwargs,
        )
    elif mode.lower() == "oracledb":
        engine = create_oracle_engine(
            conn_dict,
            custom_conn_str=custom_conn_str,
            connect_args=connect_args,
            **kwargs,
        )
    elif mode.lower() == "bigquery":
        engine = create_bigquery_engine(
            conn_dict,
            custom_conn_str=custom_conn_str,
            connect_args=connect_args,
            **kwargs,
        )

    elif mode.lower() == "mysql":
        engine = create_mysql_engine(
            conn_dict,
            custom_conn_str=custom_conn_str,
            connect_args=connect_args,
            **kwargs,
        )

    print("Uploading data...")
    # Upload data

    if method.lower() == "multi":
        try:
            print("Trying to upload data with 'multi' method...")
            response_rows_affected = df.to_sql(
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
            print(f"{type(e)} - {e}")
            try:
                print(
                    "Uploading data with 'multi' method failed. Trying to upload data with 'execute many' method..."
                )
                response_rows_affected = to_sql_executemany(
                    df, conn_dict, schema, table_name, mode, **kwargs
                )
            except Exception as e:
                print(f"{type(e)} - {e}")
                print(
                    "Uploading data with 'execute many' method failed. Trying to upload data with 'single' method..."
                )
                response_rows_affected = df.to_sql(
                    table_name,
                    engine,
                    schema=schema,
                    if_exists="append",
                    index=False,
                    chunksize=chunksize,
                    dtype=dtypes_dict,
                )
    elif method.lower() == "execute_many":
        try:
            print("Trying to upload data with 'execute many' method...")
            response_rows_affected = to_sql_executemany(
                df, conn_dict, schema, table_name, mode, **kwargs
            )
        except Exception as e:
            print(f"{type(e)} - {e}")
            print(
                "Uploading data with 'execute many' method failed. Trying to upload data with 'single' method..."
            )
            response_rows_affected = df.to_sql(
                table_name,
                engine,
                schema=schema,
                if_exists="append",
                index=False,
                chunksize=chunksize,
                dtype=dtypes_dict,
            )
    elif method.lower() == "spark":
        try:
            print("Trying to upload data with 'spark' method...")
            response_rows_affected = to_sql_redshift_spark(
                df, schema, table_name, conn_dict, mode=spark_mode
            )
        except Exception as e:
            print(f"{type(e)} - {e}")
            print(
                "Uploading data with 'spark' method failed. Trying to upload data with 'single' method..."
            )
            response_rows_affected = df.to_sql(
                table_name,
                engine,
                schema=schema,
                if_exists="append",
                index=False,
                chunksize=chunksize,
                dtype=dtypes_dict,
            )
    elif method.lower() == "single":
        print("Trying to upload data with 'single' method...")
        response_rows_affected = df.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists="append",
            index=False,
            chunksize=chunksize,
            dtype=dtypes_dict,
        )
    else:
        print("Wrong selected method. Aborting...")
        response_rows_affected = 0

    return response_rows_affected


def sql_exec_stmt(sql_stmt, conn_dict: dict, mode="pyodbc", **kwargs):
    """
    Function to execute sql statements.

    Parameters:
        sql_stmt: String with sql statement to execute.
        conn_dict: Dictionary with server, database, uid and pwd information.
        mode: String with mode to use. Options are 'pyodbc', 'redshift', 'sqlalchemy', 'oracledb' and 'bigquery'.
        **kwargs: Extra arguments for connection.

    Returns:
        response_rows_affected: Integer with number of rows affected.
        response_output: Output/result from the executed statement (if available), None otherwise.
    """

    # Set mode of connection

    if mode == "pyodbc":
        print("Connecting to database...")
        ## Make connection
        sql_conn = create_pyodbc_conn(conn_dict, **kwargs)

    elif mode == "redshift":
        print("Connecting to database...")
        ## Make connection
        sql_conn = create_redshift_conn(conn_dict, **kwargs)

    elif mode == "sqlalchemy":
        print("Connecting to database...")
        ## Make connection
        sql_conn = create_sqlalchemy_conn(conn_dict, **kwargs)

    elif mode == "oracledb":
        print("Connecting to database...")
        ## Make connection
        sql_conn = create_oracle_conn(conn_dict, **kwargs)
    elif mode == "bigquery":
        print("Connecting to database...")
        ## Make connection
        sql_conn = create_bigquery_conn(conn_dict, **kwargs)

    print("Executing statement...")
    # Execute statment
    response_output = None
    response_rows_affected = 0
    with sql_conn:
        try:
            ## Initialize cursor
            cursor = sql_conn.cursor()
            ## Execute statement
            cursor.execute(sql_stmt)
            ## Get number of rows affected
            response_rows_affected = cursor.rowcount
            ## Try to fetch results
            try:
                response_output = cursor.fetchall()
            except Exception as e:
                ## Log the exception for debugging
                print(f"Info: Could not fetch results - {type(e)}: {e}")
                response_output = None
            ## Commit changes
            sql_conn.commit()
        except Exception as e:
            ## Show error
            print(
                f"Error executing statement -> {type(e)} - {e}. Retrying without cursor..."
            )
            ## Try without cursor
            try:
                ### Execute statement
                result = sql_conn.execute(sql_stmt)
                ### Get number of rows affected
                response_rows_affected = 1
                ### Try to fetch results if available
                try:
                    response_output = result.fetchall()
                except Exception as e:
                    print(f"Info: Could not fetch results - {type(e)}: {e}")
                    response_output = None
                ### Commit changes
                sql_conn.commit()
            except Exception as e:
                ### Show error
                print(f"Error executing statement -> {type(e)} - {e}. Aborting...")
                ### Set number of rows affected
                response_rows_affected = 0

    return (
        response_rows_affected,
        response_output,
    )  # Response with number of rows affected and output


def sql_read_data(
    sql_stmt,
    conn_dict,
    custom_conn_str=None,
    connect_args={},
    mode="sqlalchemy",
    name=None,
    max_n_try=3,
    log_file_path="logs",
    **kwargs,
):
    """
    Function to read sql statements.

    Parameters:
        sql_stmt: SQL statement to execute.
        conn_dict: Dictionary with server, database, uid and pwd information.
        custom_conn_str: Custom connection string.
        mode: Mode to use. Options are 'pyodbc', 'redshift', 'sqlalchemy', 'oracledb', 'bigquery' and 'mysql'.
        connect_args: Custom connection argument.
        name: Name to use for print statements.
        max_n_try: Maximum number of tries to execute the query.
        log_file_path: Path to use for log files.
        **kwargs: Extra arguments for connection.

    Returns:
        df: Dataframe with query results.
    """

    # Read data

    t_i = dt.datetime.now()
    n_try = 0
    succeeded = False
    while n_try < max_n_try and not succeeded:
        try:
            ### Create engine
            if mode == "pyodbc":
                engine_obj = create_sqlalchemy_engine(
                    conn_dict,
                    custom_conn_str=custom_conn_str,
                    connect_args=connect_args,
                    **kwargs,
                )
            elif mode == "redshift":
                engine_obj = create_redshift_engine(
                    conn_dict,
                    custom_conn_str=custom_conn_str,
                    connect_args=connect_args,
                    **kwargs,
                )
            elif mode == "sqlalchemy":
                engine_obj = create_sqlalchemy_engine(
                    conn_dict,
                    custom_conn_str=custom_conn_str,
                    connect_args=connect_args,
                    **kwargs,
                )
            elif mode == "oracledb":
                engine_obj = create_oracle_engine(
                    conn_dict,
                    custom_conn_str=custom_conn_str,
                    connect_args=connect_args,
                    **kwargs,
                )
            elif mode == "bigquery":
                engine_obj = create_bigquery_engine(
                    conn_dict,
                    custom_conn_str=custom_conn_str,
                    connect_args=connect_args,
                    **kwargs,
                )
            elif mode == "mysql":
                engine_obj = create_mysql_engine(
                    conn_dict,
                    custom_conn_str=custom_conn_str,
                    connect_args=connect_args,
                    **kwargs,
                )
            ### Read sql statement
            df = pd.read_sql(sql_stmt, engine_obj)
            ### Dispose connections
            engine_obj.dispose()
            ### Change status
            succeeded = True
        except Exception as e:
            ### Initialize empty dataframe
            df = pd.DataFrame()
            ### Set log file name
            log_file_name = "read_data"
            ### Create logs folder
            os.makedirs(log_file_path, exist_ok=True)
            ### Create logs
            mk_err_logs(
                log_file_path,
                log_file_name,
                sys._getframe().f_code.co_name + " -> " + name,
                traceback.format_exception(
                    sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
                )[-1],
                mode="summary",
            )
            mk_err_logs(
                log_file_path,
                log_file_name,
                sys._getframe().f_code.co_name + " -> " + name,
                "".join(
                    traceback.format_exception(
                        sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
                    )
                ),
                mode="detailed",
            )
            ### Change status
            succeeded = False
        ## Update number of tries
        n_try += 1
    t_e = dt.datetime.now()
    print(f"Time elapsed in 'download' process {name} -> {df.shape} = {t_e - t_i}")

    # Create time execution logs

    ## Set log file name
    log_file_name = "download_data_texec"
    ## Create logs folder
    os.makedirs(log_file_path, exist_ok=True)
    ## Create logs
    mk_texec_logs(
        log_file_path,
        log_file_name,
        sys._getframe().f_code.co_name + " -> " + name,
        t_e - t_i,
        obs=f"Shape of object = {df.shape}",
    )

    return df


def sql_upload_data(
    df,
    schema,
    table_name,
    conn_dict,
    custom_conn_str=None,
    mode="sqlalchemy",
    connect_args={},
    name=None,
    chunksize=1000,
    method="multi",
    max_n_try=3,
    dtypes_dict={},
    n_jobs=-1,
    spark_mode="append",
    log_file_path="logs",
    **kwargs,
):
    """
    Function to upload data to database table with sqlalchemy.

    Parameters:
        df: Dataframe to upload.
        schema: Schema to upload data to.
        table_name: Table name to upload data to.
        conn_dict: Dictionary with server, database, uid and pwd information.
        custom_conn_str: String with custom connection string.
        mode: String with mode to use. Options are 'pyodbc', 'redshift', 'sqlalchemy', 'oracledb', 'bigquery' and 'mysql'.
        connect_args: Dictionary with connection arguments.
        name: Name to use for print statements.
        chunksize: Integer with chunksize to use for upload.
        method: String with method to use for upload ('multi', 'execute_many' or 'single').
        max_n_try: Integer with maximum number of tries to upload data.
        dtypes_dict: Dictionary with dtypes to use for upload.
        n_jobs: Integer with number of jobs to use for parallelization.
        spark_mode: String with mode to use when uploading to redshift with spark. Options are 'append', 'overwrite', 'ignore' and 'error'.
        log_file_path: Path to use for log files.
        **kwargs: Extra arguments for connection.

    Returns:
        response_rows_affected: Integer with number of rows affected.
    """

    # Set number of jobs

    if n_jobs == -1:
        n_jobs = multiprocessing.cpu_count()

    # Create schema if not exists (tables are automatically created by SQLAlchemy)

    try:
        ## Execute command
        response_rows_affected, response_output = sql_exec_stmt(
            DDL(f"CREATE SCHEMA IF NOT EXISTS {schema}"), conn_dict, mode=mode, **kwargs
        )
        ## Show rows affected
        print(f"Schema {schema} created -> {response_rows_affected}")
    except Exception as e:
        print(f"Error creating schema {schema} -> {type(e)} - {e}")

    # Upload data

    t_i = dt.datetime.now()
    n_try = 0
    succeeded = False
    while n_try < max_n_try and not succeeded:
        try:
            ### Show shape of dataframe
            print(f"Shape of query dataframe -> {name} = {df.shape}")
            ### Upload dataframe in parallel
            if not df.empty:
                #### Parallelize only if dataframe is large enough (avoid locking up the system for small dataframes)
                if df.shape[0] / chunksize >= n_jobs:
                    print("Uploading chunked data in parallel...")
                    ##### Split dataframe in smaller chunks
                    df_split_iter = [
                        x for x in np.array_split(df, n_jobs) if not x.empty
                    ]
                    ##### Create iterators
                    table_name_iter = [table_name for x in df_split_iter]
                    schema_iter = [schema for x in df_split_iter]
                    mode_iter = [mode for x in df_split_iter]
                    conn_dict_iter = [conn_dict for x in df_split_iter]
                    custom_conn_str_iter = [custom_conn_str for x in df_split_iter]
                    connect_args_iter = [connect_args for x in df_split_iter]
                    chunksize_iter = [chunksize for x in df_split_iter]
                    method_iter = [method for x in df_split_iter]
                    dtypes_iter = [dtypes_dict for x in df_split_iter]
                    spark_mode_iter = [spark_mode for x in df_split_iter]
                    kwargs_iter = [kwargs for x in df_split_iter]
                    ##### Get results
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

                    ##### Concatenate results
                    response_rows_affected = 0
                    for r in parallel_results:
                        response_rows_affected += r
                else:
                    print("Uploading whole data...")
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
                #### Set response rows affected to 0
                response_rows_affected = 0
            ### Show rows affected
            print(f"Affected number of rows -> {name} = {response_rows_affected}")
            ### Change status
            succeeded = True
        except Exception as e:
            ### Set log file name
            log_file_name = "upload_data"
            ### Create logs folder
            os.makedirs(log_file_path, exist_ok=True)
            ### Create logs
            mk_err_logs(
                log_file_path,
                log_file_name,
                sys._getframe().f_code.co_name + " -> " + name,
                traceback.format_exception(
                    sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
                )[-1],
                mode="summary",
            )
            mk_err_logs(
                log_file_path,
                log_file_name,
                sys._getframe().f_code.co_name + " -> " + name,
                "".join(
                    traceback.format_exception(
                        sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
                    )
                ),
                mode="detailed",
            )
            ### Set response rows affected to 0
            response_rows_affected = 0
            ### Change status
            succeeded = False
        ## Update number of tries
        n_try += 1
    t_e = dt.datetime.now()
    print(f"Time elapsed in 'upload' process {name} -> {df.shape} = {t_e - t_i}")

    # Create time execution logs

    ## Set log file name
    log_file_name = "upload_data_texec"
    ## Create logs folder
    os.makedirs(log_file_path, exist_ok=True)
    ## Create logs
    mk_texec_logs(
        log_file_path,
        log_file_name,
        sys._getframe().f_code.co_name + " -> " + name,
        t_e - t_i,
        obs=f"Shape of object = {df.shape}",
    )

    return response_rows_affected  # Response with number of rows affected


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
    Function to copy data to Redshift database from S3 bucket.

    Parameters:
        s3_file_path: String with S3 file paths to copy data from.
        schema: Schema to upload data to.
        table_name: Table name to upload data to.
        conn_dict: Dictionary with server, database, uid and pwd information.
        access_key: String with access keys for S3 bucket.
        secret_access_key: String with secret access keys for S3 bucket.
        region: String with regions for S3 bucket.
        delimiter: String with delimiter to use for copy command. Default is ','.
        header_row: Integer with header row to ignore. Default is 1.
        type_format: String with type format to use for copy command. Default is 'csv'.
        name: Name to use for print statements.
        max_n_try: Integer with maximum number of tries to upload data.
        log_file_path: Path to use for log files.
        **kwargs: Extra arguments for connection.

    Returns:
        response_rows_affected: Integer with number of rows affected.
    """

    # Copy data

    t_i = dt.datetime.now()
    n_try = 0
    succeeded = False
    while n_try < max_n_try and not succeeded:
        try:
            ### Create sql statement
            sql_stmt = f"COPY {schema}.{table_name} FROM '{s3_file_path}' ACCESS_KEY_ID '{access_key}' SECRET_ACCESS_KEY '{secret_access_key}' REGION '{region}' DELIMITER '{delimiter}' IGNOREHEADER {header_row} EMPTYASNULL FORMAT AS {type_format.upper()};"
            ### Execute copy command
            response_rows_affected, response_output = sql_exec_stmt(
                sql_stmt, conn_dict, mode="redshift", **kwargs
            )
            ### Show rows affected
            print(f"Affected number of rows -> {name} = {response_rows_affected}")
            # Change status
            succeeded = True
        except Exception as e:
            ### Set log file name
            log_file_name = "copy_data"
            ### Create logs folder
            os.makedirs(log_file_path, exist_ok=True)
            ### Create logs
            mk_err_logs(
                log_file_path,
                log_file_name,
                sys._getframe().f_code.co_name + " -> " + name,
                traceback.format_exception(
                    sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
                )[-1],
                mode="summary",
            )
            mk_err_logs(
                log_file_path,
                log_file_name,
                sys._getframe().f_code.co_name + " -> " + name,
                "".join(
                    traceback.format_exception(
                        sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
                    )
                ),
                mode="detailed",
            )
            ### Set response rows affected to 0
            response_rows_affected = 0
            ### Change status
            succeeded = False
        ## Update number of tries
        n_try += 1
    t_e = dt.datetime.now()
    print(f"Time elapsed in 'copy' process {name} = {t_e - t_i}")

    # Create time execution logs

    ## Set log file name
    log_file_name = "copy_data_texec"
    ## Create logs folder
    os.makedirs(log_file_path, exist_ok=True)
    ## Create logs
    mk_texec_logs(
        log_file_path,
        log_file_name,
        sys._getframe().f_code.co_name + " -> " + name,
        t_e - t_i,
        obs="Copy data from S3 bucket to database table",
    )

    return response_rows_affected  # Response with number of rows affected
