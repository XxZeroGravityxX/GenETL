# Import modules
import pandas as pd
import json
import time
from google.cloud import storage, bigquery
from googleapiclient import discovery
from io import BytesIO


# ============================================================================
# GCS Functions
# ============================================================================


def _parse_gcs_path(gcs_file_path):
    """
    Parse GCS file path and extract bucket name and blob path.

    Parameters:
        gcs_file_path: str. GCS file path in format 'gs://bucket-name/path/to/file'.

    Returns:
        bucket_name: str. GCS bucket name.
        blob_path: str. Blob path within the bucket.
    """
    if not gcs_file_path.startswith("gs://"):
        raise ValueError(
            f"Invalid GCS file path: {gcs_file_path}. Must start with 'gs://'"
        )

    path_parts = gcs_file_path.replace("gs://", "").split("/", 1)
    bucket_name = path_parts[0]
    blob_path = path_parts[1] if len(path_parts) > 1 else "file"

    return bucket_name, blob_path


def _get_file_format(gcs_file_path):
    """
    Extract file format from GCS file path.

    Parameters:
        gcs_file_path: str. GCS file path, can include wildcards (e.g., 'gs://bucket/path/file-*.parquet').

    Returns:
        file_format: str. File format (csv, json, parquet, etc.).
    """
    # Remove wildcard patterns to get the actual extension
    clean_path = gcs_file_path.replace("*", "")
    return clean_path.split(".")[-1].lower()


def _get_content_type(file_format):
    """
    Map file format to MIME content type.

    Parameters:
        file_format: str. File format.

    Returns:
        content_type: str. MIME content type.
    """
    content_type_map = {
        "csv": "text/csv",
        "json": "application/json",
        "parquet": "application/octet-stream",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "xls": "application/vnd.ms-excel",
    }
    return content_type_map.get(file_format, "application/octet-stream")


def _ensure_file_extension(gcs_file_path, file_format):
    """
    Ensure GCS file path has the correct extension for the file format.

    Parameters:
        gcs_file_path: str. GCS file path.
        file_format: str. File format (csv, json, parquet, avro).

    Returns:
        gcs_file_path: str. GCS file path with correct extension.
    """
    extension_map = {
        "csv": ".csv",
        "json": ".json",
        "parquet": ".parquet",
        "avro": ".avro",
    }

    extension = extension_map.get(file_format, f".{file_format}")

    # Check if path already has the correct extension
    if not gcs_file_path.endswith(extension):
        # Remove any existing extension and add the correct one
        base_path = (
            gcs_file_path.rsplit(".", 1)[0]
            if "." in gcs_file_path.split("/")[-1]
            else gcs_file_path
        )
        gcs_file_path = base_path + extension

    return gcs_file_path


def gcs_upload_file(
    data,
    gcs_file_path,
    client=None,
    file_format=None,
    encoding="utf-8",
    **kwargs,
):
    """
    Generalized function to upload data to GCS bucket.

    Parameters:
        data: pd.DataFrame or dict or bytes. Data to upload.
        gcs_file_path: str. GCS file path in format 'gs://bucket-name/path/to/file.ext'.
        client: google.cloud.storage.Client. Optional. GCS storage client.
        file_format: str. File format (csv, json, parquet, xlsx). If None, inferred from file extension.
        encoding: str. Encoding to use. Default 'utf-8'.
        **kwargs: Additional arguments (sep, index for CSV; orient for JSON; etc.).

    Returns:
        result: dict. Dictionary containing upload information including file_path, bucket, blob,
                file_size_bytes, file_format, and status.
    """
    ## Parse GCS path
    bucket_name, blob_path = _parse_gcs_path(gcs_file_path)

    ## Infer file format if not provided
    if file_format is None:
        file_format = _get_file_format(gcs_file_path)
    file_format = file_format.lower()

    ## Create client if not provided
    if client is None:
        client = storage.Client()

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    ## Convert data based on format
    if file_format == "csv":
        csv_buffer = BytesIO()
        sep = kwargs.pop("sep", ",")
        index = kwargs.pop("index", False)
        data.to_csv(csv_buffer, sep=sep, index=index, encoding=encoding, **kwargs)
        csv_buffer.seek(0)
        content = csv_buffer.getvalue()

    elif file_format == "json":
        if isinstance(data, pd.DataFrame):
            orient = kwargs.pop("orient", "records")
            json_str = data.to_json(orient=orient)
        else:
            json_str = json.dumps(data)
        content = json_str.encode(encoding)

    elif file_format == "parquet":
        parquet_buffer = BytesIO()
        data.to_parquet(parquet_buffer, **kwargs)
        parquet_buffer.seek(0)
        content = parquet_buffer.getvalue()

    elif file_format == "xlsx":
        xlsx_buffer = BytesIO()
        data.to_excel(xlsx_buffer, **kwargs)
        xlsx_buffer.seek(0)
        content = xlsx_buffer.getvalue()

    elif isinstance(data, bytes):
        content = data

    else:
        raise ValueError(
            f"Unsupported file format: {file_format}. Supported formats: csv, json, parquet, xlsx."
        )

    ## Upload to GCS
    content_type = _get_content_type(file_format)
    blob.upload_from_string(content, content_type=content_type)

    ## Return upload information
    return {
        "file_path": gcs_file_path,
        "bucket": bucket_name,
        "blob": blob_path,
        "file_size_bytes": len(content),
        "file_format": file_format,
        "status": "uploaded",
    }


def gcs_download_file(
    gcs_file_path,
    client=None,
    file_format=None,
    return_bytes=False,
    **kwargs,
):
    """
    Generalized function to download files from GCS bucket.

    Parameters:
        gcs_file_path: str. GCS file path in format 'gs://bucket-name/path/to/file.ext'.
        client: google.cloud.storage.Client. Optional. GCS storage client.
        file_format: str. File format (csv, json, parquet, xlsx). If None, inferred from file extension.
        return_bytes: bool. If True, return raw bytes instead of parsed data. Default False.
        **kwargs: Additional arguments (passed to pandas read functions).

    Returns:
        result: dict. Dictionary containing the data and metadata including data, file_path, bucket,
                blob, file_size_bytes, file_format, and return_type.
    """
    ## Parse GCS path
    bucket_name, blob_path = _parse_gcs_path(gcs_file_path)

    ## Infer file format if not provided
    if file_format is None:
        file_format = _get_file_format(gcs_file_path)
    file_format = file_format.lower()

    ## Create client if not provided
    if client is None:
        client = storage.Client()

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    ## Download from GCS
    file_data = blob.download_as_bytes()

    if return_bytes:
        return {
            "data": file_data,
            "file_path": gcs_file_path,
            "bucket": bucket_name,
            "blob": blob_path,
            "file_size_bytes": len(file_data),
            "file_format": file_format,
            "return_type": "bytes",
        }

    ## Parse data based on format
    parsed_data = None
    if file_format == "csv":
        csv_buffer = BytesIO(file_data)
        parsed_data = pd.read_csv(csv_buffer, **kwargs)

    elif file_format == "json":
        parsed_data = json.loads(file_data.decode("utf-8"))

    elif file_format == "parquet":
        parquet_buffer = BytesIO(file_data)
        parsed_data = pd.read_parquet(parquet_buffer, **kwargs)

    elif file_format == "xlsx":
        xlsx_buffer = BytesIO(file_data)
        parsed_data = pd.read_excel(xlsx_buffer, **kwargs)

    else:
        raise ValueError(
            f"Unsupported file format: {file_format}. Supported formats: csv, json, parquet, xlsx."
        )

    ## Return download information with parsed data
    return {
        "data": parsed_data,
        "file_path": gcs_file_path,
        "bucket": bucket_name,
        "blob": blob_path,
        "file_size_bytes": len(file_data),
        "file_format": file_format,
        "return_type": file_format,
    }


def gcs_delete_files(
    gcs_path,
    client=None,
    prefix_match=None,
):
    """
    Delete files from GCS bucket recursively.

    Parameters:
        gcs_path: str. GCS path in format 'gs://bucket-name/path/to/folder/' or 'gs://bucket-name/path/to/file.ext'.
        client: google.cloud.storage.Client. Optional. GCS storage client.
        prefix_match: str. Optional. Delete only files matching this prefix within the gcs_path.

    Returns:
        deleted_count: int. Number of files deleted.
    """
    ## Parse GCS path
    bucket_name, blob_path = _parse_gcs_path(gcs_path)

    ## Create client if not provided
    if client is None:
        client = storage.Client()

    bucket = client.bucket(bucket_name)

    ## Determine the prefix to list
    list_prefix = blob_path
    if prefix_match:
        list_prefix = (
            f"{blob_path.rstrip('/')}/{prefix_match}" if blob_path else prefix_match
        )

    ## List and delete all blobs matching the prefix
    blobs = bucket.list_blobs(prefix=list_prefix)
    deleted_count = 0

    for blob in blobs:
        blob.delete()
        deleted_count += 1

    return deleted_count


# ============================================================================
# BigQuery Functions
# ============================================================================


def bigquery_to_gcs(
    project_id,
    dataset_id,
    table_id,
    gcs_file_path,
    file_format="csv",
    bq_client=None,
    **kwargs,
):
    """
    Export data from a BigQuery table to GCS.

    Parameters:
        project_id: str. GCP project ID.
        dataset_id: str. BigQuery dataset ID.
        table_id: str. BigQuery table ID.
        gcs_file_path: str. GCS file path in format 'gs://bucket-name/path/to/file.ext'.
                       For parquet/avro with multiple files, use wildcard: 'gs://bucket/path/file-*.parquet'
        file_format: str. Export format (csv, json, parquet, avro). Default 'csv'.
        bq_client: google.cloud.bigquery.Client. Optional. BigQuery client.
        **kwargs: Additional arguments for extract_table job (e.g., compression, field_delimiter).

    Returns:
        job: google.cloud.bigquery.ExtractJob. BigQuery extract job result.
    """
    if bq_client is None:
        bq_client = bigquery.Client(project=project_id)

    file_format = file_format.lower()
    table_id = f"{project_id}.{dataset_id}.{table_id}"

    ## Ensure destination path has correct file extension (unless using wildcard for multi-file export)
    if "*" not in gcs_file_path:
        gcs_file_path = _ensure_file_extension(gcs_file_path, file_format)
    else:
        # For wildcard paths, verify the extension matches the format
        path_format = _get_file_format(gcs_file_path)
        if path_format != file_format:
            print(
                f"WARNING: Wildcard path format '{path_format}' doesn't match file_format '{file_format}'."
            )
            print(
                f"BigQuery will export in '{path_format}' format based on the file extension."
            )

    ## Create extract job config
    extract_config = bigquery.ExtractJobConfig()

    ## Set destination format
    format_map = {
        "csv": bigquery.DestinationFormat.CSV,
        "json": bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
        "parquet": bigquery.DestinationFormat.PARQUET,
        "avro": bigquery.DestinationFormat.AVRO,
    }
    extract_config.destination_format = format_map.get(
        file_format, bigquery.DestinationFormat.CSV
    )

    ## Apply additional kwargs to extract config
    for key, value in kwargs.items():
        if hasattr(extract_config, key):
            setattr(extract_config, key, value)

    ## Create and run extract job
    print(f"Exporting to: {gcs_file_path}")
    print(f"Format (from extension): {_get_file_format(gcs_file_path)}")

    extract_job = bq_client.extract_table(
        table_id, gcs_file_path, job_config=extract_config
    )
    extract_job.result()

    return extract_job


def gcs_to_bigquery(
    gcs_file_path,
    project_id,
    dataset_id,
    table_id,
    file_format=None,
    write_disposition="WRITE_TRUNCATE",
    autodetect=True,
    bq_client=None,
    **kwargs,
):
    """
    Import data from GCS to a BigQuery table.

    Parameters:
        gcs_file_path: str. GCS file path in format 'gs://bucket-name/path/to/file.ext'.
                       Can use wildcards for multiple files: 'gs://bucket/path/file-*.csv'
        project_id: str. GCP project ID.
        dataset_id: str. BigQuery dataset ID.
        table_id: str. BigQuery table ID.
        file_format: str. File format (csv, json, parquet, avro). If None, inferred from file extension.
                    IMPORTANT: Must match the actual file content format, not just the extension.
        write_disposition: str. Write disposition (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY). Default 'WRITE_TRUNCATE'.
        autodetect: bool. Auto-detect schema from data (useful for CSV). Default True.
        bq_client: google.cloud.bigquery.Client. Optional. BigQuery client.
        **kwargs: Additional arguments for load_table_from_uri job.

    Returns:
        job: google.cloud.bigquery.LoadJob. BigQuery load job.
    """
    if bq_client is None:
        bq_client = bigquery.Client(project=project_id)

    if file_format is None:
        file_format = _get_file_format(gcs_file_path)
    file_format = file_format.lower()

    table_id = f"{project_id}.{dataset_id}.{table_id}"

    ## Create load job config
    load_config = bigquery.LoadJobConfig()

    ## Set source format
    format_map = {
        "csv": bigquery.SourceFormat.CSV,
        "json": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        "parquet": bigquery.SourceFormat.PARQUET,
        "avro": bigquery.SourceFormat.AVRO,
    }
    load_config.source_format = format_map.get(file_format, bigquery.SourceFormat.CSV)

    ## Skip leading rows
    if file_format == "csv":
        load_config.skip_leading_rows = (
            0 if autodetect else 1
        )  # If autodetect is True, don't skip the first row so BigQuery can use it for column names

    ## Set autodetect for schema inference (especially useful for CSV)
    load_config.autodetect = autodetect

    ## Convert string write_disposition to enum if needed
    if isinstance(write_disposition, str):
        write_disposition_map = {
            "WRITE_TRUNCATE": bigquery.WriteDisposition.WRITE_TRUNCATE,
            "WRITE_APPEND": bigquery.WriteDisposition.WRITE_APPEND,
            "WRITE_EMPTY": bigquery.WriteDisposition.WRITE_EMPTY,
        }
        write_disposition = write_disposition_map.get(
            write_disposition, bigquery.WriteDisposition.WRITE_TRUNCATE
        )
    load_config.write_disposition = write_disposition

    ## Apply additional kwargs to load config
    for key, value in kwargs.items():
        if hasattr(load_config, key):
            setattr(load_config, key, value)

    # Create and run load job
    load_job = bq_client.load_table_from_uri(
        gcs_file_path, table_id, job_config=load_config
    )
    load_job.result()

    return load_job


# ============================================================================
# Cloud SQL Functions
# ============================================================================


def cloud_sql_to_gcs(
    project_id,
    instance_id,
    database,
    gcs_file_path,
    file_type="SQL",
    wait_for_completion=True,
    poll_interval=5,
    **kwargs,
):
    """
    Export data from a Cloud SQL database to GCS using Cloud SQL Admin API.

    Parameters:
        project_id: str. GCP project ID.
        instance_id: str. Cloud SQL instance ID.
        database: str. Database name to export.
        gcs_file_path: str. GCS file path in format 'gs://bucket-name/path/to/file.sql' or 'gs://bucket-name/path/to/file.csv'.
        file_type: str. Export format ('SQL' or 'CSV'). Default 'SQL'.
        wait_for_completion: bool. Whether to wait for the operation to complete. Default True.
        poll_interval: int. Seconds to wait between status checks. Default 5.
        **kwargs: Additional arguments for export context (e.g., offload, csvExportOptions).

    Returns:
        result: dict. Dictionary containing operation details including operation_id, operation_type,
                status, instance, database, destination, file_type, and operation_response.
    """
    ## Parse GCS path
    bucket_name, blob_path = _parse_gcs_path(gcs_file_path)
    gcs_uri = f"gs://{bucket_name}/{blob_path}"

    ## Construct the service object for the Cloud SQL Admin API
    service = discovery.build("sqladmin", "v1")

    ## Build export context
    export_context = {
        "fileType": file_type,
        "uri": gcs_uri,
        "databases": [database],
    }
    export_context.update(kwargs)

    request_body = {"exportContext": export_context}

    ## Execute the export operation
    request = service.instances().export(
        project=project_id, instance=instance_id, body=request_body
    )
    response = request.execute()
    operation_id = response["name"]

    if not wait_for_completion:
        return {
            "operation_id": operation_id,
            "operation_type": "export",
            "status": "running",
            "instance": instance_id,
            "database": database,
            "destination": gcs_uri,
            "file_type": file_type,
            "operation_response": response,
        }

    ## Monitor the operation status
    while True:
        op_request = service.operations().get(
            project=project_id, operation=operation_id
        )
        op_response = op_request.execute()
        if op_response["status"] == "DONE":
            if "error" in op_response:
                raise Exception(f"Export failed: {op_response['error']}")
            return {
                "operation_id": operation_id,
                "operation_type": "export",
                "status": "completed",
                "instance": instance_id,
                "database": database,
                "destination": gcs_uri,
                "file_type": file_type,
                "operation_response": op_response,
            }
        time.sleep(poll_interval)


def gcs_to_cloud_sql(
    gcs_file_path,
    project_id,
    instance_id,
    database,
    file_type="SQL",
    wait_for_completion=True,
    poll_interval=5,
    **kwargs,
):
    """
    Import data from GCS to a Cloud SQL database using Cloud SQL Admin API.

    Parameters:
        gcs_file_path: str. GCS file path in format 'gs://bucket-name/path/to/file.sql' or 'gs://bucket-name/path/to/file.csv'.
        project_id: str. GCP project ID.
        instance_id: str. Cloud SQL instance ID.
        database: str. Database name to import into.
        file_type: str. Import format ('SQL' or 'CSV'). Default 'SQL'.
        wait_for_completion: bool. Whether to wait for the operation to complete. Default True.
        poll_interval: int. Seconds to wait between status checks. Default 5.
        **kwargs: Additional arguments for import context (e.g., csvImportOptions, sqlImportOptions).

    Returns:
        result: dict. Dictionary containing operation details including operation_id, operation_type,
                status, instance, database, source, file_type, and operation_response.
    """
    ## Parse GCS path
    bucket_name, blob_path = _parse_gcs_path(gcs_file_path)
    gcs_uri = f"gs://{bucket_name}/{blob_path}"

    ## Construct the service object for the Cloud SQL Admin API
    service = discovery.build("sqladmin", "v1")

    ## Build import context
    import_context = {
        "fileType": file_type,
        "uri": gcs_uri,
        "database": database,
    }
    import_context.update(kwargs)

    request_body = {"importContext": import_context}

    ## Execute the import operation
    request = service.instances().import_(
        project=project_id, instance=instance_id, body=request_body
    )
    response = request.execute()
    operation_id = response["name"]

    if not wait_for_completion:
        return {
            "operation_id": operation_id,
            "operation_type": "import",
            "status": "running",
            "instance": instance_id,
            "database": database,
            "source": gcs_uri,
            "file_type": file_type,
            "operation_response": response,
        }

    ## Monitor the operation status
    while True:
        op_request = service.operations().get(
            project=project_id, operation=operation_id
        )
        op_response = op_request.execute()
        if op_response["status"] == "DONE":
            if "error" in op_response:
                raise Exception(f"Import failed: {op_response['error']}")
            return {
                "operation_id": operation_id,
                "operation_type": "import",
                "status": "completed",
                "instance": instance_id,
                "database": database,
                "source": gcs_uri,
                "file_type": file_type,
                "operation_response": op_response,
            }
        time.sleep(poll_interval)
