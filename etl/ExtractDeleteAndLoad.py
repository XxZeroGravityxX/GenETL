# Import modules
import datetime as dt
import numpy as np
import pandas as pd
import sqlalchemy

# Import custom modules
from ..tools.sql import sql_exec_stmt, sql_read_data, sql_upload_data, sql_copy_data
from ..tools.aws import dynamodb_read_data, s3_upload_csv


class ExtractDeleteAndLoad(object):
    """
    Class with routines to read, delete and/or upload data from/to a database or data storage.
    """

    def __init__(
        self,
        config_dict={},
        conn_dict={},
        sqlalchemy_dict={},
    ):
        """
        Class constructor.

        Parameters:

        config_dict : dict. Configuration dictionary with connection and data parameters.
        conn_dict : dict. Connection dictionary with connection parameters.
        sqlalchemy_dict : dict. Dictionary with sqlalchemy data types.
        """

        ## Set class parameters

        # Set configuration parameters
        self.connections_dict = conn_dict
        self.configs_dict = config_dict
        self.sqlalchemy_dtypes = sqlalchemy_dict
        # Set processes names
        processes_list = ["download", "delete", "truncate", "upload"]
        # Set connection parameters
        self.conn_info_dict = {key: {} for key in processes_list}
        self.conn_suff_dict = {key: {} for key in processes_list}
        self.conn_type_dict = {key: {} for key in processes_list}
        # Iterate over download connections
        for p_name in processes_list:
            for key in self.configs_dict[f"{p_name}_connections_dict"].keys():
                # Get connection suffix
                self.conn_suff_dict[p_name][key] = self.configs_dict[
                    f"{p_name}_connections_dict"
                ][key].split("_")[-1]
                # Get connection type
                self.conn_type_dict[p_name][key] = self.configs_dict[
                    f"{p_name}_connections_dict"
                ][key].split("_")[0]
                # Get connection dictionary
                self.conn_info_dict[p_name][key] = {
                    "myoracle_client_dir": (
                        self.connections_dict["myoracle_client_dir"]
                        if "myoracle_client_dir" in self.connections_dict.keys()
                        else ""
                    ),
                    "myserver": (
                        self.connections_dict[
                            f"myserver_{self.conn_type_dict[p_name][key]}_{self.conn_suff_dict[p_name][key]}"
                        ]
                        if f"myserver_{self.conn_type_dict[p_name][key]}_{self.conn_suff_dict[p_name][key]}"
                        in self.connections_dict.keys()
                        else ""
                    ),
                    "mydatabase": (
                        self.connections_dict[
                            f"mydatabase_{self.conn_type_dict[p_name][key]}_{self.conn_suff_dict[p_name][key]}"
                        ]
                        if f"mydatabase_{self.conn_type_dict[p_name][key]}_{self.conn_suff_dict[p_name][key]}"
                        in self.connections_dict.keys()
                        else ""
                    ),
                    "myusername": (
                        self.connections_dict[
                            f"myusername_{self.conn_type_dict[p_name][key]}_{self.conn_suff_dict[p_name][key]}"
                        ]
                        if f"myusername_{self.conn_type_dict[p_name][key]}_{self.conn_suff_dict[p_name][key]}"
                        in self.connections_dict.keys()
                        else ""
                    ),
                    "mypassword": (
                        self.connections_dict[
                            f"mypassword_{self.conn_type_dict[p_name][key]}_{self.conn_suff_dict[p_name][key]}"
                        ]
                        if f"mypassword_{self.conn_type_dict[p_name][key]}_{self.conn_suff_dict[p_name][key]}"
                        in self.connections_dict.keys()
                        else ""
                    ),
                    "mycharset": (
                        self.connections_dict[
                            f"mycharset_{self.conn_type_dict[p_name][key]}_{self.conn_suff_dict[p_name][key]}"
                        ]
                        if f"mycharset_{self.conn_type_dict[p_name][key]}_{self.conn_suff_dict[p_name][key]}"
                        in self.connections_dict.keys()
                        else ""
                    ),
                    "myencoding": (
                        self.connections_dict[
                            f"awmyencoding_{self.conn_type_dict[p_name][key]}_{self.conn_suff_dict[p_name][key]}"
                        ]
                        if f"awmyencoding_{self.conn_type_dict[p_name][key]}_{self.conn_suff_dict[p_name][key]}"
                        in self.connections_dict.keys()
                        else ""
                    ),
                }

    def delete_data(self, **kwargs):
        """
        Function to delete data from the database.

        Parameters:

        kwargs : dict. Keyword arguments to pass to the delete statement.
        """

        # Set keyword arguments as variables
        for key, val in kwargs.items():
            locals()[key] = val
        # Iterate over delete statements
        for key, stmt in self.configs_dict["delete_sql_stmts_dict"].items():
            # Start date to ingest data from
            global start_date
            start_date = (
                eval(self.configs_dict["delete_dates_dict"][key]["start_date"])
                if "eval(" in self.configs_dict["delete_dates_dict"][key]["start_date"]
                else self.configs_dict["delete_dates_dict"][key]["start_date"]
            )
            # End date to ingest data from
            global end_date
            end_date = (
                eval(self.configs_dict["delete_dates_dict"][key]["end_date"])
                if "eval(" in self.configs_dict["delete_dates_dict"][key]["end_date"]
                else self.configs_dict["delete_dates_dict"][key]["end_date"]
            )
            print(f"Deleting data from {key}, since {start_date} to {end_date}...")
            # Get connection suffix
            conn_suff = self.conn_suff_dict["delete"][key]
            # Get connection type
            conn_type = self.conn_type_dict["delete"][key]
            # Get connection dictionary
            conn_dict = self.conn_info_dict["delete"][key]
            # Execute delete statement
            print(f"Deleting data for {key}...")
            try:
                sql_exec_stmt(
                    eval(stmt) if "{" in stmt else stmt, conn_dict, mode=conn_type
                )
            except Exception as e:
                print(f"Error deleting data: {type(e)} - {e}")

        pass

    def truncate_data(self, **kwargs):
        """
        Function to truncate data from the database.
        """

        # Iterate over truncate statements
        for key, stmt in self.configs_dict["truncate_sql_stmts_dict"].items():
            # Get connection suffix
            conn_suff = self.conn_suff_dict["truncate"][key]
            # Get connection type
            conn_type = self.conn_type_dict["truncate"][key]
            # Get connection dictionary
            conn_dict = self.conn_info_dict["truncate"][key]
            # Execute truncate statement
            print(f"Truncating data for {key}...")
            try:
                sql_exec_stmt(
                    eval(stmt) if "{" in stmt else stmt, conn_dict, mode=conn_type
                )
            except Exception as e:
                print(f"Error truncating data: {type(e)} - {e}")

        pass

    def read_data(self):
        """
        Function to read CMV data from DynamoDB.
        """

        # Initialize raw data
        self.raw_data = {}
        # Iterate over tables/statements
        for key, tb_name in self.configs_dict["download_table_names_dict"].items():
            # Start date to ingest data from
            global start_date
            start_date = (
                eval(self.configs_dict["download_dates_dict"][key]["start_date"])
                if "eval("
                in self.configs_dict["download_dates_dict"][key]["start_date"]
                else self.configs_dict["download_dates_dict"][key]["start_date"]
            )
            # End date to ingest data from
            global end_date
            end_date = (
                eval(self.configs_dict["download_dates_dict"][key]["end_date"])
                if "eval(" in self.configs_dict["download_dates_dict"][key]["end_date"]
                else self.configs_dict["download_dates_dict"][key]["end_date"]
            )
            print(f"Reading data from {key}, since {start_date} to {end_date}...")
            # Get connection suffix
            conn_suff = self.conn_suff_dict["download"][key]
            # Get connection type
            conn_type = self.conn_type_dict["download"][key]
            # Get connection dictionary
            conn_dict = self.conn_info_dict["download"][key]
            # Evaluate keyword arguments
            kwargs_eval = {
                kw_key: eval(kw_val) if "{" in kw_val else kw_val
                for kw_key, kw_val in self.configs_dict["download_kwargs_dict"][
                    key
                ].items()
            }
            # Read data
            if conn_type == "dynamodb":
                data = dynamodb_read_data(
                    tb_name,
                    self.connections_dict[f"aws_access_key_id_{conn_suff}"],
                    self.connections_dict[f"aws_secret_access_key_{conn_suff}"],
                    self.connections_dict[f"region_name_{conn_suff}"],
                    **kwargs_eval,
                )
            else:
                data = sql_read_data(
                    (
                        eval(self.configs_dict["download_sql_stmts"][key])
                        if "{" in self.configs_dict["download_sql_stmts"][key]
                        else self.configs_dict["download_sql_stmts"][key]
                    ),
                    conn_dict,
                    mode=conn_type,
                    custom_conn_str=self.configs_dict["download_custom_conn_strs_dict"][
                        key
                    ],
                    connect_args=self.configs_dict["download_connect_args_dict"][key],
                    name=tb_name,
                    max_n_try=3,
                )
            # Add data to raw data dictionary
            self.raw_data[key] = data.copy()
        # Free memory
        del data

        pass

    def upload_data(self, data_to_upload: dict):
        """
        Function to upload CMV data into the database.

        Parameters:

        data_to_upload : list. List with data to upload.
        """

        # Iterate over tables/statements
        for key, tb_name in self.configs_dict["upload_tables_dict"].items():
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
                pass
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
                if "redshift" in conn_type:
                    # Upload data to S3
                    s3_upload_csv(
                        upload_data,
                        self.configs_dict["s3_file_paths_dict"][key],
                        self.connections_dict[f"aws_access_key_id_{conn_suff}"],
                        self.connections_dict[f"aws_secret_access_key_{conn_suff}"],
                        region_name=self.connections_dict[f"region_name_{conn_suff}"],
                        sep=",",
                        index=False,
                        encoding="utf-8",
                    )
                    # Copy data from S3 to database
                    sql_copy_data(
                        (
                            eval(self.configs_dict["s3_file_paths_dict"][key])
                            if "{" in self.configs_dict["s3_file_paths_dict"][key]
                            else self.configs_dict["s3_file_paths_dict"][key]
                        ),
                        self.configs_dict["upload_schemas_dict"][key],
                        self.configs_dict["upload_tables_dict"][key],
                        conn_dict,
                        self.connections_dict[f"aws_access_key_id_aws_{conn_suff}"],
                        self.connections_dict[f"aws_secret_access_key_aws_{conn_suff}"],
                        self.connections_dict[f"region_name_aws_{conn_suff}"],
                        name=self.configs_dict["upload_tables_dict"][key],
                        max_n_try=self.configs_dict["max_n_try"],
                    )
                else:
                    # Upload data to database
                    sql_upload_data(
                        upload_data,
                        self.configs_dict["upload_schemas_dict"][key],
                        self.configs_dict["upload_tables_dict"][key],
                        conn_dict,
                        mode=conn_type,
                        custom_conn_str=self.configs_dict[
                            "upload_custom_conn_strs_dict"
                        ][key],
                        connect_args=self.configs_dict["upload_connect_args_dict"][key],
                        name=self.configs_dict["upload_tables_dict"][key],
                        chunksize=self.configs_dict["upload_chunksizes_dict"][key],
                        method=self.configs_dict["upload_methods_dict"][key],
                        dtypes_dict=dtypes_dict,
                        max_n_try=self.configs_dict["max_n_try"],
                        n_jobs=self.configs_dict["n_parallel"],
                    )

        pass
