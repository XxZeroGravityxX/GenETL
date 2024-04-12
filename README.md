# GenETL

## Where to get it

The source code is hosted on GitHub at: https://github.com/XxZeroGravityxX/GenETL. Binary installers for the latest released version are available at the [Python Package Index (PyPI)](https://pypi.org/project/GenETL) 

```bash
# or PyPi
pip install GenETL
```

## Dependencies

Main dependencies are listed below:

```
awswrangler
boto3
colorama
numpy
pandas
PyYAML
tqdm
oracledb
redshift-connector
PyMySQL
SQLAlchemy
sqlalchemy-redshift
pyodbc
cx-Oracle
pyspark
```

## Licence

[MIT](https://en.wikipedia.org/wiki/MIT_License)

## Documentation

The configuration for main class (**ExtractDeleteAndLoad)** methods to work, are defined on dictionaries, with connection, data and other parameters. Such configurations are listed below (as YAML and JSON files), with the corresponding arguments names passed to the class:

### Configuration parameters

```yaml
## Delete parameters

# Delete connections
delete_connections_dict:
  key_name: <connection-type>_<connection-name>  # Same as in connections dictionary
# SQL delete statements
delete_sql_stmts_dict:
  key_name: <sql-delete-statement>
# Set extra variables to use for data deletion
delete_extra_vars_dict:
  key_name:
    var1: <user-defined-variable>
    var2: <user-defined-variable>

## Download Parameters

# Download connections
download_connections_dict:
  key_name: <connection-type>_<connection-name>  # Same as in connections dictionary
# SQL table names
download_table_names_dict:
  key_name: <table_name>
# SQL download statements
download_sql_stmts_dict:
  key_name: <sql-download-statement>
# Keyword arguments (for DynamoDB download method only)
download_dynamodb_kwargs_dict:
  key_name: <kwarg-dynamo>
# Set extra variables to use for data download
download_extra_vars_dict:
  key_name:
    var1: <user-defined-variable>
    var2: <user-defined-variable>

## Upload Parameters

# Upload connections
upload_connections_dict:
  key_name: <connection-type>_<connection-name>  # Same as in connections dictionary
upload_schemas_dict:
  key_name: <schema>
upload_tables_dict:
  key_name: <table_name>
# Upload metaparameters
upload_chunksizes_dict:
  key_name: <chunk-size>
# Upload data types
upload_python_to_sql_dtypes_dict:
  key_name:
    var1: <sql-datatype>
    var2: <sql-datatype>
# Upload S3 parameters (for Redshift upload (COPY) method or CSV upload only)
s3_file_paths_dict: <aws-s3-bucket>
s3_csv_seps_dict: <csv-separator>
s3_csv_encodings_dict: <csv-encoding-type>
```

### Connection parameters

```yaml
## Resources

# Oracle client dir
myoracle_client_dir: <oracle-client-path>

## Database Connections

# Connection 1
# <connection-type>: oracledb, <connection-name>: conn1 <=> myserver_oracledb_conn1, mydatabase_oracledb_conn1, etc.
myserver_oracledb_conn1: <host-ip-or-url>
mydatabase_oracledb_conn1: <database-name>
myusername_oracledb_conn1: <database-username>
mypassword_oracledb_conn1: <database-password>
# Connection 2
# <connection-type>: redshift, <connection-name>: conn2 <=> myserver_redshift_conn2, mydatabase_redshift_conn2, etc.
myserver_redshift_conn2: <host-ip-or-url>
mydatabase_redshift_conn2: <database-name>
myusername_redshift_conn2: <database-username>
mypassword_redshift_conn2: <database-password>
# ...

## AWS Credentials

# AWS Credentials 1
# <credentials-name>: dev <=> aws_access_key_id_dev, aws_secret_access_key_dev, etc.)
aws_access_key_id_dev: <aws-iam-access-key>
aws_secret_access_key_dev: <aws-iam-secret-access-key>
region_name_dev: <aws-region-name>
# AWS Credentials 2
# <credentials-name>: prod <=> aws_access_key_id_prod, aws_secret_access_key_prod, etc.)
aws_access_key_id_prod: <aws-iam-access-key>
aws_secret_access_key_prod: <aws-iam-secret-access-key>
region_name_prod: <aws-region-name>
# ...

```

### SQLalchemy data types

```json
{
    "varchar": "sqlalchemy.types.String",
    "timestamp": "sqlalchemy.types.DateTime",
    "int": "sqlalchemy.types.Numeric",
    "float": "sqlalchemy.types.Float",
    "varchar2": "sqlalchemy.types.String",
    "number": "sqlalchemy.types.Numeric"
}
```

- etl.edl
    - class **ExtractDeleteAndLoad**(object)
        - **__init__**(self, config_dict={}, conn_dict={}, sqlalchemy_dict={})
        Class constructor. 
        
        Parameters: 
        
        config_dict : dict. Configuration dictionary with connection and data parameters.
        conn_dict : dict. Connection dictionary with connection parameters.
        sqlalchemy_dict : dict. Dictionary with sqlalchemy data types.
        - **delete_data**(self, **kwargs)
        Function to delete data from the database. 
        
        Parameters: 
        kwargs : dict. Keyword arguments to pass to the delete statement.
        - **read_data**(self, **kwargs)
        Function to read data from the source.
        - **truncate_data**(self, **kwargs)
        Function to truncate data from the source.
        - **upload_data**(self, data_to_upload: dict, **kwargs)
        Function to upload data to the target. 
        
        Parameters: 
        
        data_to_upload : list. List with data to upload.
- etl_tools.aws
    - **dynamodb_read_data**(table_name, aws_access_key_id, aws_secret_access_key, region_name, **kwargs)
    Function to read data from DynamoDB.
    - **s3_get_object**(s3_bucket_name, s3_path, aws_access_key, aws_secret_access_key, region_name='us-east-1')
    Function to get object from S3 bucket. 
    
    Parameters:
    
    s3_bucket_name: str. Name of the S3 bucket without "s3://"
    prefix.s3_path: str. Path to the file in the S3 bucket (relative to root).
    aws_access_key: str. Name of the environment variable with the AWS access key.
    aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
    region_name: str. Name of the AWS region to use.
    - **s3_list_objects**(s3_bucket_name, s3_path, aws_access_key, aws_secret_access_key, region_name='us-east-1')
    Function to list objects from S3 bucket.
    
    Parameters:
    
    s3_bucket_name: str. Name of the S3 bucket without "s3://"
    prefix.s3_path: str. Path to the file in the S3 bucket (relative to root).
    aws_access_key: str. Name of the environment variable with the AWS access key.
    aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
    region_name: str. Name of the AWS region to use.
    - **s3_put_object**(s3_body_content, s3_bucket_name, s3_path, aws_access_key, aws_secret_access_key, region_name='us-east-1')
    Function to put object on S3 bucket.
    
    Parameters:
    
    s3_body_content: bytes. Content to be uploaded to S3.
    s3_bucket_name: str. Name of the S3 bucket without "s3://"
    prefix.s3_path: str. Path to the file in the S3 bucket (relative to root).
    aws_access_key: str. Name of the environment variable with the AWS access key
    .aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
    region_name: str. Name of the AWS region to use.
    - **s3_read_csv**(s3_bucket_name, s3_path, aws_access_key, aws_secret_access_key, region_name='us-east-1', **kwargs)
    Function to read csv from S3 bucket. 
    
    Parameters:
    
    s3_bucket_name: str. Name of the S3 bucket without "s3://"
    prefix.s3_path: str. Path to the csv file in the S3 bucket (relative to root).
    aws_access_key: str. Name of the environment variable with the AWS access key.
    aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
    region_name: str. Name of the AWS region to use.
    kwargs: dict. Keyword arguments to pass to pd.read_csv.
    - **s3_read_file**(s3_bucket_name, s3_path, aws_access_key, aws_secret_access_key, region_name='us-east-1', encoding='utf-8', file_type='plain')
    Function to read .csv or .json file from S3 bucket.
    
    Parameters:
    
    s3_bucket_name: str. Name of the S3 bucket without "s3://" 
    prefix.s3_path: str. Path to the file in the S3 bucket (relative to root).
    aws_access_key: str. Name of the environment variable with the AWS access key.
    aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
    region_name: str. Name of the AWS region to use.
    encoding:  str. Encoding to use for reading the file.
    file_type: str. Type of file to read ("csv" or "plain"). Default "plain".
    - **s3_read_json**(s3_bucket_name, s3_path, aws_access_key, aws_secret_access_key, region_name='us-east-1', encoding='utf-8')
    Function to read json from S3 bucket. 
    
    Parameters:
    
    s3_bucket_name: str. Name of the S3 bucket without "s3://"
    prefix.s3_path: str. Path to the json file in the S3 bucket (relative to root).
    aws_access_key: str. Name of the environment variable with the AWS access key.
    aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
    region_name: str. Name of the AWS region to use.
    - **s3_read_pkl**(s3_bucket_name, s3_pickle_path, aws_access_key, aws_secret_access_key, region_name='us-east-1')
    Function to read pickle file from S3.
    
    Parameters:
    
    s3_bucket_name: str. Name of the S3 bucket without "s3://"
    prefix.s3_pickle_path: str. Path to the pickle file in the S3 bucket (relative to bucket).
    aws_access_key: str. Name of the environment variable with the AWS access key.
    aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
    region_name: str. Name of the AWS region to use.
    - **s3_upload_csv**(data, s3_file_path, aws_access_key, aws_secret_access_key, region_name='us-east-1', sep=',', index=False, encoding='utf-8')
    Function to upload data as CSV to S3 bucket.
    
    Parameters:
    
    data: pd.DataFrame. Data to upload.
    s3_file_path: str. S3 file path.
    aws_access_key: str. Name of the environment variable with the AWS access key.
    aws_secret_access_key:  str. Name of the environment variable with the AWS secret access key.
    region_name: str. Name of the AWS region to use.
    sep: str. Separator to use for CSV data.
    index: bool. Whether to include the index in the file.
    encoding: str. Encoding to use.
    - **s3_write_json**(json_data, s3_bucket_name, s3_path, aws_access_key, aws_secret_access_key, region_name='us-east-1', encoding='utf-8')
    Function to write json to S3 bucket.
    
    Parameters:
    
    json_data: dict. Data to be written to json file.
    s3_bucket_name: str. Name of the S3 bucket without "s3://" prefix.
    s3_path: str. Path to the json file in the S3 bucket (relative to root).
    aws_access_key: str. Name of the environment variable with the AWS access key.
    aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
    region_name: str. Name of the AWS region to use.
    - **s3_write_parquet**(data, s3_bucket_name, s3_path, aws_access_key, aws_secret_access_key, region_name='us-east-1')
    Function to write DataFrame to .parquet in S3 bucket.
    
    Parameters:
    
    data: pd.DataFrame. Data to be written to .parquet file.
    s3_bucket_name: str. Name of the S3 bucket without "s3://" prefix.
    s3_path: str. Path to the .parquet file in the S3 bucket (relative to root).
    aws_access_key: str. Name of the environment variable with the AWS access key.
    aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
    region_name: str. Name of the AWS region to use.
- etl_tools.execution
    - **execute_script**(process_str, log_file_path='logs', exec_log_file_name='exec.log', texec_log_file_name='txec.log')
    Function to execute an script, saving execution logs.
    
    Parameters:
    
    process_str : String. Process to execute.
    log_file_path : String. File path to use for saving logs.
    exec_log_file_name : String. Execution log file name.
    texec_log_file_name : String. Time execution log file name.
    - **mk_err_logs**(file_path, file_name, err_var, err_desc, mode='summary')
    Function to create/save log error files.
    
    Parameters:
    
    file_path : String. File path to use for saving logs.
    file_name : String. File name to use for log file.
    err_desc : String. Error description.
    err_var : String. Error variable name.
    - **mk_exec_logs**(file_path, file_name, process_name, output_content)
    Function to create/save execution log files.
    
    Parameters:
    
    file_path : String. File path to use for saving logs.
    file_name : String. File name to use for log file.
    process_name: String. Process name.
    output_content: String. Output content.
    - **mk_texec_logs**(file_path, file_name, time_var, time_val, obs=None)
    Function to create/save log time execution files.
    
    Parameters:
    
    file_path : String. File path to use for saving logs.
    file_name : String. File name to use for log file.
    time_val : String. Time variable's value.
    time_var : String. Time variable's name.
    - **parallel_execute**(applyFunc, *args, **kwargs)
    Function to execute function parallely.
    
    Parameters:
    
    applyFunc : Function. Function to apply parallely.
    args: Iterable. Arguments to pass to function on each parallel execution.
- etl_tools.sql
    - **create_mysql_engine**(conn_dict: dict)
    Function to create mysql engine from connection dictionary.
    
    Parameters:
    
    conn_dict: Dictionary with server, database, uid and pwd information.
    - **create_oracle_conn**(conn_dict: dict)
    Function to create oracle connector from connection dictionary.
    
    Parameters:
    
    conn_dict: Dictionary with server, database, uid and pwd information.
    - **create_oracle_engine**(conn_dict: dict)
    Function to create oracle engine from connection dictionary.
    
    Parameters:
    
    conn_dict: Dictionary with server, database, uid and pwd information.
    - **create_pyodbc_conn**(conn_dict: dict)
    Function to create pyodbc connector from connection dictionary.
    
    Parameters:
    
    conn_dict: Dictionary with server, database, uid and pwd information.
    - **create_redshift_conn**(conn_dict: dict)
    Function to create redshift connector from connection dictionary.
    
    Parameters:
    
    conn_dict: Dictionary with server, database, uid and pwd information.
    - **create_redshift_engine**(conn_dict: dict)
    Function to create redshift engine from connection dictionary.
    
    Parameters:
    
    conn_dict: Dictionary with server, database, uid and pwd information.
    - **create_sqlalchemy_conn**(conn_dict: dict, custom_conn_str=None)
    Function to create sqlalchemy connector from connection dictionary.
    
    Parameters:
    
    conn_dict: Dictionary with server, database, uid and pwd information.
    custom_conn_str: String with custom connection string.
    - **create_sqlalchemy_engine**(conn_dict: dict, custom_conn_str=None, connect_args={})
    Function to create sqlalchemy enginefrom connection dictionary.
    
    Parameters:
    
    conn_dict: Dictionary with server, database, uid and pwd information.
    custom_conn_str: String with custom connection string.
    connect_args: Dictionary with extra arguments for connection.
    - **parallel_to_sql**(df, table_name, schema, mode, conn_dict, custom_conn_str, connect_args, chunksize, method, dtypes_dict, spark_mode='append')
    Function to upload data to database table with sqlalchemy in parallel. 
    
    Parameters:
    
    df : Pandas dataframe with data to upload.
    table_name : String with table name to upload data.engine :                    SQLAlchemy engine.
    schema : String with schema name.
    mode : String with mode to use. Options are 'sqlalchemy', 'redshift' and 'oracledb.
    conn_dict : Dictionary with server, database, uid and pwd information.
    custom_conn_str : String with custom connection string.
    connect_args : Dictionary with extra arguments for connection.
    chunksize : Integer with chunksize to use.
    method : String with method to use ('multi', 'execute_many', 'spark' or 'single').
    dtypes_dict : Dictionary with dtypes to use for upload.
    spark_mode : String with mode to use when uploading to redshift with spark. Options are 'append', 'overwrite', 'ignore' and 'error'.
    - **sql_copy_data**(s3_file_path, schema, table_name, conn_dict, access_key, secret_access_key, region, delimiter=',', header_row=1, type_format='csv', name=None, max_n_try=3)
    Function to copy data to Redshift database from S3 bucket.
    
    Parameters:
    
    s3_file_path: String with S3 file paths to copy data from.
    schema: Schema to upload data to.
    table_name: Table name to upload data to.
    conn_dict: Dictionarie with server, database, uid and pwd information.
    access_key: String with access keys for S3 bucket.
    secret_access_key: String with secret access keys for S3 bucket.
    region: String with regions for S3 bucket.
    delimiter: String with delimiter to use for copy command. Default is ','.
    header_row: Integer with header row to ignore. Default is 1.
    type_format: String with type format to use for copy command. Default is 'csv'.
    name: Name to use for print statements.
    max_n_try: Integer with maximum number of tries to upload data.
    - **sql_exec_stmt**(sql_stmt, conn_dict: dict, mode='pyodbc')
    Function to execute sql statements.
    
    Parameters:
    
    sql_stmt : String with sql statement to execute.
    conn_dict : Dictionary with server, database, uid and pwd information.
    mode : String with mode to use. Options are 'pyodbc' and 'redshift'.
    - **sql_read_data**(sql_stmt, conn_dict, mode='sqlalchemy', custom_conn_str=None, connect_args={}, name=None, max_n_try=3)
    Function to read sql statements.
    
    Parameters:
    
    sql_stmt : SQL statement to execute.
    conn_dict : Dictionary with server, database, uid and pwd information.
    mode : Mode to use. Options are 'sqlalchemy', 'redshift' and 'oracledb'.
    custom_conn_str : Custom connection string.
    connect_args : Custom connection argument.
    name : Name to use for print statements.
    max_n_try : Maximum number of tries to execute the query.
    - **sql_upload_data**(df, schema, table_name, conn_dict, mode='sqlalchemy', custom_conn_str=None, connect_args={}, name=None, chunksize=1000, method='multi', max_n_try=3, dtypes_dict={}, n_jobs=-1, spark_mode='append')
    Function to upload data to database table with sqlalchemy.
    
    Parameters:
    
    df : Dataframe to upload.
    schema : Schema to upload data to.
    table_name : Table name to upload data to.
    conn_dict : Dictionarie with server, database, uid and pwd information.
    mode : String with mode to use. Options are 'sqlalchemy' and 'redshift'.
    custom_conn_str : String with custom connection string.
    connect_args : Dictionarie with connection arguments.
    name : Name to use for print statements.
    chunksize : Integer with chunksize to use for upload.
    method : String with method to use for upload ('multi', 'execute_many' or 'single').
    max_n_try : Integer with maximum number of tries to upload data.
    dtypes_dict : Dictionarie with dtypes to use for upload.
    n_jobs : Integer with number of jobs to use for parallelization.
    spark_mode : String with mode to use when uploading to redshift with spark. Options are 'append', 'overwrite', 'ignore' and 'error'.
    - **to_sql_executemany**(data, conn_dict, schema, table_name, mode)
    Function to upload data to database table with sqlalchemy in parallel.
    
    Parameters:
    
    data : Pandas dataframe with data to upload.
    conn_dict : Dictionary with server, database, uid and pwd information.
    schema : String with schema name.
    table_name : String with table name to upload data.
    mode : String with mode to use. Options are 'pyodbc' and 'redshift'.
    - **to_sql_redshift_spark**(data, schema, table_name, conn_dict, mode='append')
    Function to upload data to redshift with spark.
    
    Parameters:
    
    data : Pandas dataframe with data to upload.
    schema : String with schema name.
    table_name : String with table name to upload data.
    conn_dict : Dictionary with server, database, uid and pwd information.
    mode : String with mode to use. Options are 'append', 'overwrite', 'ignore' and 'error'.