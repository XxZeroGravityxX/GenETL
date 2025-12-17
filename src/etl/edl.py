# Import modules
import datetime as dt
import numpy as np
import pandas as pd
import sqlalchemy

# Import custom modules
from etl_tools.sql import sql_exec_stmt, sql_read_data, sql_upload_data, sql_copy_data
from etl_tools.aws import dynamodb_read_data, s3_upload_csv
from etl_tools.api import API_request


class ExtractDeleteAndLoad(object):
    """
    Class with routines to read, delete and/or upload data from/to a database or data storage.
    """

    def __init__(
        self,
        config_dict={},
        conn_dict={},
        sqlalchemy_dict={},
        globals_dict={},
        locals_dict={},
    ):
        """
        Class constructor.

        Parameters:

        config_dict :           dict. Configuration dictionary with connection and data parameters. Should/could have
                                the following keys for each process:
                                    - <process_name>_connections_dict
                                    - <process_name>_extra_vars_dict
                                    - <process_name>_sql_stmts_dict
                                    - <process_name>_tables_dict
                                    - <process_name>_dynamodb_kwargs_dict
                                    - <process_name>_urls_dict
                                    - <process_name>_headers_dict
                                    - <process_name>_params_dict
                                    - <process_name>_datas_dict
                                    - <process_name>_jsons_dict
                                    - <process_name>_request_types_dict
        conn_dict :             dict. Connection dictionary with connection information. Should/could have
                                the following keys for each connection:
                                    - oracle_client_dir
                                    - server
                                    - database
                                    - username
                                    - password
                                    - charset
                                    - encoding
                                    - location
                                    - engine_prefix
                                    - port
                                    - sslmode
                                    - driver
                                    - url
                                    - key
                                    - secret
        sqlalchemy_dict :       dict. Dictionary with sqlalchemy data types.
        globals_dict :          dict. Global variables dictionary.
        locals_dict :           dict. Local variables dictionary.
        """

        ## Set class parameters

        # Set configuration parameters (lowercase keys)
        self.connections_dict = {key.lower(): val for key, val in conn_dict.items()}
        self.configs_dict = {key.lower(): val for key, val in config_dict.items()}
        self.sqlalchemy_dtypes = {
            key.lower(): val for key, val in sqlalchemy_dict.items()
        }
        # Set global variables
        for key, val in globals_dict.items():
            # Set as global
            globals()[key] = val
        # Set local variables
        for key, val in locals_dict.items():
            locals()[key] = val
        # Set processes names
        processes_list = ["download", "delete", "truncate", "upload"]
        # Set connection parameters
        self.conn_info_dict = {key: {} for key in processes_list}
        self.conn_suff_dict = {key: {} for key in processes_list}
        self.conn_type_dict = {key: {} for key in processes_list}
        # Iterate over processes
        for p_name in processes_list:
            # Skip if process is not in configuration dictionary
            if f"{p_name}_connections_dict" not in self.configs_dict.keys():
                continue
            # Iterate over connections
            for key, val in self.configs_dict[f"{p_name}_connections_dict"].items():
                # Get connection suffix
                self.conn_suff_dict[p_name][key] = val.split("_")[-1]
                # Get connection type
                self.conn_type_dict[p_name][key] = val.split("_")[0]
                # Get connection dictionary
                self.conn_info_dict[p_name][key] = self.connections_dict[val]

    def delete_data(self, **kwargs):
        """
        Function to delete data from the source.

        Parameters:

        kwargs : dict. Keyword arguments to pass to the delete statement.
        """

        # Check if delete configurations are set
        if (
            ("delete" not in self.conn_suff_dict.keys())
            or ("delete" not in self.conn_type_dict.keys())
            or ("delete" not in self.conn_info_dict.keys())
        ):
            raise ValueError(
                "Delete configurations are not set! Please set them first."
            )
        # Iterate over connections
        for key in self.configs_dict["delete_connections_dict"].keys():
            print(f"Deleting data for {key}...")
            # Set extra variables as global variables
            if "delete_extra_vars_dict" in self.configs_dict.keys():
                if key in self.configs_dict["delete_extra_vars_dict"].keys():
                    if len(self.configs_dict["delete_extra_vars_dict"][key]) > 0:
                        # Turn on variable evaluation
                        evaluate_vars = True
                        # Evaluate extra variables
                        for ex_key, ex_val in self.configs_dict[
                            "delete_extra_vars_dict"
                        ][key].items():
                            globals()[ex_key] = (
                                eval(ex_val) if "eval(" in ex_val else ex_val
                            )
                    else:
                        # Turn off variable evaluation
                        evaluate_vars = False
                else:
                    # Turn off variable evaluation
                    evaluate_vars = False
            else:
                # Turn off variable evaluation
                evaluate_vars = False
            # Get connection suffix
            conn_suff = self.conn_suff_dict["delete"][key]
            # Get connection type
            conn_type = self.conn_type_dict["delete"][key]
            # Get connection dictionary
            conn_dict = self.conn_info_dict["delete"][key]
            # Delete data
            if conn_type == "dynamodb":
                pass
            elif conn_type == "s3":
                pass
            elif conn_type == "api":
                api_response = API_request(
                    self.configs_dict["delete_urls_dict"][key],
                    headers=(
                        self.configs_dict["delete_headers_dict"][key]
                        if "delete_headers_dict" in self.configs_dict.keys()
                        else None
                    ),
                    params=(
                        self.configs_dict["delete_params_dict"][key]
                        if "delete_params_dict" in self.configs_dict.keys()
                        else None
                    ),
                    data=(
                        self.configs_dict["delete_datas_dict"][key]
                        if "delete_datas_dict" in self.configs_dict.keys()
                        and key in self.configs_dict["delete_datas_dict"].keys()
                        else None
                    ),
                    json=(
                        self.configs_dict["delete_jsons_dict"][key]
                        if "delete_jsons_dict" in self.configs_dict.keys()
                        and key in self.configs_dict["delete_jsons_dict"].keys()
                        else None
                    ),
                    request_type=(self.configs_dict["delete_request_types_dict"][key]),
                )
                print(f"     API response: {api_response}")
            else:
                # Get delete statement
                stmt = (
                    eval(self.configs_dict["delete_sql_stmts_dict"][key])
                    if (
                        "{" in self.configs_dict["delete_sql_stmts_dict"][key]
                        and evaluate_vars
                    )
                    else self.configs_dict["delete_sql_stmts_dict"][key]
                )
                print(f"     Delete query: {stmt}")
                # Execute delete statement
                try:
                    rows_affected, output = sql_exec_stmt(
                        stmt,
                        conn_dict,
                        mode=conn_type,
                        **{k: v for k, v in kwargs.items() if k not in ("mode")},
                    )
                except Exception as e:
                    print(f"Error deleting data: {type(e)} - {e}")

        pass

    def truncate_data(self, **kwargs):
        """
        Function to truncate data from the source.
        """

        # Check if truncate configurations are set
        if (
            ("truncate" not in self.conn_suff_dict.keys())
            or ("truncate" not in self.conn_type_dict.keys())
            or ("truncate" not in self.conn_info_dict.keys())
        ):
            raise ValueError(
                "Truncate configurations are not set! Please set them first."
            )
        # Iterate over connections
        for key in self.configs_dict["truncate_connections_dict"].keys():
            print(f"Truncating data for {key}...")
            # Set extra variables as global variables
            if "truncate_extra_vars_dict" in self.configs_dict.keys():
                if key in self.configs_dict["truncate_extra_vars_dict"].keys():
                    if len(self.configs_dict["truncate_extra_vars_dict"][key]) > 0:
                        # Turn on variable evaluation
                        evaluate_vars = True
                        # Evaluate extra variables
                        for ex_key, ex_val in self.configs_dict[
                            "truncate_extra_vars_dict"
                        ][key].items():
                            globals()[ex_key] = (
                                eval(ex_val) if "eval(" in ex_val else ex_val
                            )
                    else:
                        # Turn off variable evaluation
                        evaluate_vars = False
                else:
                    # Turn off variable evaluation
                    evaluate_vars = False
            else:
                # Turn off variable evaluation
                evaluate_vars = False
            # Get connection suffix
            conn_suff = self.conn_suff_dict["truncate"][key]
            # Get connection type
            conn_type = self.conn_type_dict["truncate"][key]
            # Get connection dictionary
            conn_dict = self.conn_info_dict["truncate"][key]
            # Truncate data
            if conn_type == "dynamodb":
                pass
            elif conn_type == "s3":
                pass
            elif conn_type == "api":
                pass
            else:
                # Get truncate statement
                stmt = (
                    eval(self.configs_dict["truncate_sql_stmts_dict"][key])
                    if (
                        "{" in self.configs_dict["truncate_sql_stmts_dict"][key]
                        and evaluate_vars
                    )
                    else self.configs_dict["truncate_sql_stmts_dict"][key]
                )
                print(f"     Truncate query: {stmt}")
                # Execute truncate statement
                try:
                    rows_affected, output = sql_exec_stmt(
                        stmt,
                        conn_dict,
                        mode=(
                            conn_type if "mode" not in kwargs.keys() else kwargs["mode"]
                        ),
                        **{k: v for k, v in kwargs.items() if k not in ("mode")},
                    )
                except Exception as e:
                    print(f"Error truncating data: {type(e)} - {e}")

        pass

    def read_data(self, **kwargs):
        """
        Function to read data from the source.
        """

        # Check if download configurations are set
        if (
            ("download" not in self.conn_suff_dict.keys())
            or ("download" not in self.conn_type_dict.keys())
            or ("download" not in self.conn_info_dict.keys())
        ):
            raise ValueError(
                "Download configurations are not set! Please set them first."
            )
        # Initialize raw data
        self.raw_data = {}
        # Iterate over connections
        for key in self.configs_dict["download_connections_dict"].keys():
            print(f"Downloading data for {key}...")
            # Set extra variables as global variables
            if "download_extra_vars_dict" in self.configs_dict.keys():
                if key in self.configs_dict["download_extra_vars_dict"].keys():
                    if len(self.configs_dict["download_extra_vars_dict"][key]) > 0:
                        # Turn on variable evaluation
                        evaluate_vars = True
                        # Evaluate extra variables
                        for ex_key, ex_val in self.configs_dict[
                            "download_extra_vars_dict"
                        ][key].items():
                            globals()[ex_key] = (
                                eval(ex_val) if "eval(" in ex_val else ex_val
                            )
                    else:
                        # Turn off variable evaluation
                        evaluate_vars = False
                else:
                    # Turn off variable evaluation
                    evaluate_vars = False
            else:
                # Turn off variable evaluation
                evaluate_vars = False
            # Get connection suffix
            conn_suff = self.conn_suff_dict["download"][key]
            # Get connection type
            conn_type = self.conn_type_dict["download"][key]
            # Get connection dictionary
            conn_dict = self.conn_info_dict["download"][key]
            # Read data
            if conn_type == "dynamodb":
                print(
                    f"     DynamoDB table: {self.configs_dict['download_tables_dict'][key]}"
                )
                # Download data from DynamoDB
                data = dynamodb_read_data(
                    self.configs_dict["download_tables_dict"][key],
                    self.connections_dict[f"aws_access_key_id_{conn_suff}"],
                    self.connections_dict[f"aws_secret_access_key_{conn_suff}"],
                    self.connections_dict[f"region_name_{conn_suff}"],
                    **kwargs,
                )
            elif conn_type == "s3":
                pass
            elif conn_type == "api":
                print(
                    f"     API endpoint: {self.configs_dict['download_urls_dict'][key]}"
                )
                # Download data from API
                data = API_request(
                    self.configs_dict["download_urls_dict"][key],
                    headers=(
                        (
                            self.configs_dict["download_headers_dict"][key]
                            if "download_headers_dict" in self.configs_dict.keys()
                            else None
                        )
                        if "headers" not in kwargs.keys()
                        else kwargs["headers"]
                    ),
                    params=(
                        (
                            self.configs_dict["download_params_dict"][key]
                            if "download_params_dict" in self.configs_dict.keys()
                            else None
                        )
                        if "params" not in kwargs.keys()
                        else kwargs["params"]
                    ),
                    data=(
                        (
                            self.configs_dict["download_datas_dict"][key]
                            if "download_datas_dict" in self.configs_dict.keys()
                            and key in self.configs_dict["download_datas_dict"].keys()
                            else None
                        )
                        if "data" not in kwargs.keys()
                        else kwargs["data"]
                    ),
                    json=(
                        (
                            self.configs_dict["download_jsons_dict"][key]
                            if "download_jsons_dict" in self.configs_dict.keys()
                            and key in self.configs_dict["download_jsons_dict"].keys()
                            else None
                        )
                        if "json" not in kwargs.keys()
                        else kwargs["json"]
                    ),
                    request_type=(
                        (self.configs_dict["download_request_types_dict"][key])
                        if "request_type" not in kwargs.keys()
                        else kwargs["request_type"]
                    ),
                    **{
                        k: v
                        for k, v in kwargs.items()
                        if k
                        not in (
                            "headers",
                            "params",
                            "data",
                            "json",
                            "request_type",
                        )
                    },
                )
            else:
                # Get download statement
                stmt = (
                    eval(self.configs_dict["download_sql_stmts_dict"][key])
                    if (
                        "{" in self.configs_dict["download_sql_stmts_dict"][key]
                        and evaluate_vars
                    )
                    else self.configs_dict["download_sql_stmts_dict"][key]
                )
                print(f"     Download query: {stmt}")
                # Download data
                data = sql_read_data(
                    stmt,
                    conn_dict,
                    custom_conn_str=(
                        (
                            self.configs_dict["download_custom_conn_strs_dict"][key]
                            if "download_custom_conn_strs_dict"
                            in self.configs_dict.keys()
                            else None
                        )
                        if "custom_conn_str" not in kwargs.keys()
                        else kwargs["custom_conn_str"]
                    ),
                    mode=(conn_type if "mode" not in kwargs.keys() else kwargs["mode"]),
                    connect_args=(
                        (
                            self.configs_dict["download_connect_args_dict"][key]
                            if "download_connect_args_dict" in self.configs_dict.keys()
                            else {}
                        )
                        if "connect_args" not in kwargs.keys()
                        else kwargs["connect_args"]
                    ),
                    name=(
                        (
                            self.configs_dict["download_tables_dict"][key]
                            if "download_tables_dict" in self.configs_dict.keys()
                            else key
                        )
                        if "name" not in kwargs.keys()
                        else kwargs["name"]
                    ),
                    max_n_try=(
                        (
                            self.configs_dict["max_n_try"]
                            if "max_n_try" in self.configs_dict.keys()
                            else 3
                        )
                        if "max_n_try" not in kwargs.keys()
                        else kwargs["max_n_try"]
                    ),
                    log_file_path=(
                        (
                            self.configs_dict["log_file_path"]
                            if "log_file_path" in self.configs_dict.keys()
                            else "logs"
                        )
                        if "log_file_path" not in kwargs.keys()
                        else kwargs["log_file_path"]
                    ),
                    **{
                        k: v
                        for k, v in kwargs.items()
                        if k
                        not in (
                            "custom_conn_str",
                            "mode",
                            "connect_args",
                            "name",
                            "max_n_try",
                            "log_file_path",
                        )
                    },
                )
            # Add data to raw data dictionary
            self.raw_data[key] = data.copy()
        # Free memory
        del data

        pass

    def upload_data(self, data_to_upload: dict, **kwargs):
        """
        Function to upload data to the target.

        Parameters:

        data_to_upload : list. List with data to upload.
        """

        # Check if upload configurations are set
        if (
            ("upload" not in self.conn_suff_dict.keys())
            or ("upload" not in self.conn_type_dict.keys())
            or ("upload" not in self.conn_info_dict.keys())
        ):
            raise ValueError(
                "Upload configurations are not set! Please set them first."
            )
        # Iterate over connections
        for key in self.configs_dict["upload_connections_dict"].keys():
            print(f"Uploading data for {key}...")
            # Set data to upload
            upload_data = data_to_upload[key]
            # Get connection suffix
            conn_suff = self.conn_suff_dict["upload"][key]
            # Get connection type
            conn_type = self.conn_type_dict["upload"][key]
            # Get connection dictionary
            conn_dict = self.conn_info_dict["upload"][key]
            # Upload data
            if conn_type == "dynamodb":
                print(
                    f"     DynamoDB table: {self.configs_dict['upload_tables_dict'][key]}"
                )
                # Upload data to DynamoDB
                pass
            elif conn_type == "s3":
                print(f"     S3 bucket: {self.configs_dict['s3_file_paths_dict'][key]}")
                # Upload data to S3
                s3_upload_csv(
                    upload_data,
                    self.configs_dict["s3_file_paths_dict"][key],
                    self.connections_dict[f"aws_access_key_id_{conn_suff}"],
                    self.connections_dict[f"aws_secret_access_key_{conn_suff}"],
                    region_name=(
                        self.connections_dict[f"region_name_{conn_suff}"]
                        if "region_name" not in kwargs.keys()
                        else kwargs["region_name"]
                    ),
                    sep=(
                        (
                            self.configs_dict["s3_csv_seps_dict"][key]
                            if "s3_csv_seps_dict" in self.configs_dict.keys()
                            else ","
                        )
                        if "sep" not in kwargs.keys()
                        else kwargs["sep"]
                    ),
                    index=(False if "index" not in kwargs.keys() else kwargs["index"]),
                    encoding=(
                        (
                            self.configs_dict["s3_csv_encodings_dict"][key]
                            if "s3_csv_encodings_dict" in self.configs_dict.keys()
                            else "utf-8"
                        )
                        if "encoding" not in kwargs.keys()
                        else kwargs["encoding"]
                    ),
                    **{
                        k: v
                        for k, v in kwargs.items()
                        if k
                        not in (
                            "region_name",
                            "sep",
                            "index",
                            "encoding",
                        )
                    },
                )
            elif conn_type == "api":
                print(
                    f"     API endpoint: {self.configs_dict['upload_urls_dict'][key]}"
                )
                # Upload data to API
                api_response = API_request(
                    self.configs_dict["upload_urls_dict"][key],
                    headers=(
                        (
                            self.configs_dict["upload_headers_dict"][key]
                            if "upload_headers_dict" in self.configs_dict.keys()
                            else None
                        )
                        if "headers" not in kwargs.keys()
                        else kwargs["headers"]
                    ),
                    data=(
                        (
                            upload_data
                            if "upload_datas_dict" in self.configs_dict.keys()
                            and key in self.configs_dict["upload_datas_dict"].keys()
                            else None
                        )
                        if "data" not in kwargs.keys()
                        else kwargs["data"]
                    ),
                    json=(
                        (
                            upload_data
                            if "upload_jsons_dict" in self.configs_dict.keys()
                            and key in self.configs_dict["upload_jsons_dict"].keys()
                            else None
                        )
                        if "json" not in kwargs.keys()
                        else kwargs["json"]
                    ),
                    request_type=(
                        self.configs_dict["upload_request_types_dict"][key]
                        if "request_type" not in kwargs.keys()
                        else kwargs["request_type"]
                    ),
                    **{
                        k: v
                        for k, v in kwargs.items()
                        if k
                        not in (
                            "headers",
                            "data",
                            "json",
                            "request_type",
                        )
                    },
                )
                print(f"     API response: {api_response}")
            else:
                # Define column names and data types
                col_dict = self.configs_dict["upload_python_to_sql_dtypes_dict"][key]
                # Define dictionary with data types
                dtypes_dict = {
                    col: (
                        eval(
                            f'{self.sqlalchemy_dtypes[col_dtype.split("(")[0]]}({col_dtype.split("(")[1][:-1]})'
                        )
                        if "(" in col_dtype
                        else eval(f"{self.sqlalchemy_dtypes[col_dtype]}()")
                    )
                    for col, col_dtype in col_dict.items()
                }
                # Use order defined in data types dictionary
                upload_data = upload_data[list(col_dict.keys())]
                # Upload data to created tables
                if "redshift" in conn_type:  # Upload data to Redshift
                    print(
                        f"     Redshift table: {self.configs_dict['upload_tables_dict'][key]}"
                    )
                    # Upload data to S3
                    s3_upload_csv(
                        upload_data,
                        self.configs_dict["s3_file_paths_dict"][key],
                        self.connections_dict[f"aws_access_key_id_{conn_suff}"],
                        self.connections_dict[f"aws_secret_access_key_{conn_suff}"],
                        region_name=(
                            self.connections_dict[f"region_name_{conn_suff}"]
                            if "region_name" not in kwargs.keys()
                            else kwargs["region_name"]
                        ),
                        sep=(
                            (
                                self.configs_dict["s3_csv_seps_dict"][key]
                                if "s3_csv_seps_dict" in self.configs_dict.keys()
                                else ","
                            )
                            if "sep" not in kwargs.keys()
                            else kwargs["sep"]
                        ),
                        index=(
                            False if "index" not in kwargs.keys() else kwargs["index"]
                        ),
                        encoding=(
                            (
                                self.configs_dict["s3_csv_encodings_dict"][key]
                                if "s3_csv_encodings_dict" in self.configs_dict.keys()
                                else "utf-8"
                            )
                            if "encoding" not in kwargs.keys()
                            else kwargs["encoding"]
                        ),
                        **{
                            k: v
                            for k, v in kwargs.items()
                            if k
                            not in (
                                "region_name",
                                "sep",
                                "index",
                                "encoding",
                            )
                        },
                    )
                    # Copy data from S3 to database
                    sql_copy_data(
                        self.configs_dict["s3_file_paths_dict"][key],
                        self.configs_dict["upload_schemas_dict"][key],
                        self.configs_dict["upload_tables_dict"][key],
                        conn_dict,
                        self.connections_dict[f"aws_access_key_id_aws_{conn_suff}"],
                        self.connections_dict[f"aws_secret_access_key_aws_{conn_suff}"],
                        self.connections_dict[f"region_name_aws_{conn_suff}"],
                        name=(
                            self.configs_dict["upload_tables_dict"][key]
                            if "name" not in kwargs.keys()
                            else kwargs["name"]
                        ),
                        max_n_try=(
                            (
                                self.configs_dict["max_n_try"]
                                if "max_n_try" in self.configs_dict.keys()
                                else 3
                            )
                            if "max_n_try" not in kwargs.keys()
                            else kwargs["max_n_try"]
                        ),
                        log_file_path=(
                            (
                                self.configs_dict["log_file_path"]
                                if "log_file_path" in self.configs_dict.keys()
                                else "logs"
                            )
                            if "log_file_path" not in kwargs.keys()
                            else kwargs["log_file_path"]
                        ),
                        **{
                            k: v
                            for k, v in kwargs.items()
                            if k
                            not in (
                                "name",
                                "max_n_try",
                                "log_file_path",
                            )
                        },
                    )
                else:  # Upload data to other databases
                    print(
                        f"     {conn_type.capitalize()} table: {self.configs_dict['upload_tables_dict'][key]}"
                    )
                    # Upload data to database
                    sql_upload_data(
                        upload_data,
                        self.configs_dict["upload_schemas_dict"][key],
                        self.configs_dict["upload_tables_dict"][key],
                        conn_dict,
                        custom_conn_str=(
                            (
                                self.configs_dict["upload_custom_conn_strs_dict"][key]
                                if "upload_custom_conn_strs_dict"
                                in self.configs_dict.keys()
                                else None
                            )
                            if "custom_conn_str" not in kwargs.keys()
                            else kwargs["custom_conn_str"]
                        ),
                        mode=(
                            conn_type if "mode" not in kwargs.keys() else kwargs["mode"]
                        ),
                        connect_args=(
                            (
                                self.configs_dict["upload_connect_args_dict"][key]
                                if "upload_connect_args_dict"
                                in self.configs_dict.keys()
                                else {}
                            )
                            if "connect_args" not in kwargs.keys()
                            else kwargs["connect_args"]
                        ),
                        name=(
                            self.configs_dict["upload_tables_dict"][key]
                            if "name" not in kwargs.keys()
                            else kwargs["name"]
                        ),
                        chunksize=(
                            (
                                self.configs_dict["upload_chunksizes_dict"][key]
                                if "upload_chunksizes_dict" in self.configs_dict.keys()
                                else 100
                            )
                            if "chunksize" not in kwargs.keys()
                            else kwargs["chunksize"]
                        ),
                        method=(
                            (
                                self.configs_dict["upload_methods_dict"][key]
                                if "upload_methods_dict" in self.configs_dict.keys()
                                else "multi"
                            )
                            if "method" not in kwargs.keys()
                            else kwargs["method"]
                        ),
                        dtypes_dict=(
                            dtypes_dict
                            if "dtypes_dict" not in kwargs.keys()
                            else kwargs["dtypes_dict"]
                        ),
                        max_n_try=(
                            (
                                self.configs_dict["max_n_try"]
                                if "max_n_try" in self.configs_dict.keys()
                                else 3
                            )
                            if "max_n_try" not in kwargs.keys()
                            else kwargs["max_n_try"]
                        ),
                        n_jobs=(
                            (
                                self.configs_dict["n_parallel"]
                                if "n_parallel" in self.configs_dict.keys()
                                else -1
                            )
                            if "n_jobs" not in kwargs.keys()
                            else kwargs["n_jobs"]
                        ),
                        log_file_path=(
                            (
                                self.configs_dict["log_file_path"]
                                if "log_file_path" in self.configs_dict.keys()
                                else "logs"
                            )
                            if "log_file_path" not in kwargs.keys()
                            else kwargs["log_file_path"]
                        ),
                        **{
                            k: v
                            for k, v in kwargs.items()
                            if k
                            not in (
                                "custom_conn_str",
                                "mode",
                                "connect_args",
                                "name",
                                "chunksize",
                                "method",
                                "dtypes_dict",
                                "max_n_try",
                                "n_jobs",
                                "log_file_path",
                            )
                        },
                    )

        pass
