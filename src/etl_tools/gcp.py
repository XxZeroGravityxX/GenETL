# Import modules
import pandas as pd
import json
from google.cloud import storage
from io import BytesIO


def gcs_upload_csv(
    data,
    gcs_file_path,
    client=None,
    sep=",",
    index=False,
    encoding="utf-8",
    **kwargs,
):
    """
    Function to upload data as CSV to GCS bucket.

    Parameters:

    data:               pd.DataFrame. Data to upload.
    gcs_file_path:      str. GCS file path in format 'gs://bucket-name/path/to/file.csv'.
    client:             google.cloud.storage.Client. Optional. GCS storage client. If not provided, a new one will be created.
    sep:                str. Separator to use for CSV data. Default ','.
    index:              bool. Whether to include the index in the file. Default False.
    encoding:           str. Encoding to use. Default 'utf-8'.
    **kwargs:           Additional arguments to pass to pd.to_csv().
    """

    # Parse GCS file path
    if not gcs_file_path.startswith("gs://"):
        raise ValueError(
            f"Invalid GCS file path: {gcs_file_path}. Must start with 'gs://'"
        )

    # Extract bucket name and blob path
    path_parts = gcs_file_path.replace("gs://", "").split("/", 1)
    bucket_name = path_parts[0]
    blob_path = path_parts[1] if len(path_parts) > 1 else "file.csv"

    # Create client if not provided
    if client is None:
        client = storage.Client()

    # Get bucket
    bucket = client.bucket(bucket_name)

    # Get blob
    blob = bucket.blob(blob_path)

    # Convert DataFrame to CSV bytes
    csv_buffer = BytesIO()
    data.to_csv(csv_buffer, sep=sep, index=index, encoding=encoding, **kwargs)
    csv_buffer.seek(0)

    # Upload to GCS
    blob.upload_from_string(
        csv_buffer.getvalue(),
        content_type="text/csv",
    )

    pass


def gcs_upload_json(
    json_data,
    gcs_file_path,
    client=None,
    encoding="utf-8",
    **kwargs,
):
    """
    Function to upload JSON data to GCS bucket.

    Parameters:

    json_data:          dict or pd.DataFrame. Data to upload as JSON.
    gcs_file_path:      str. GCS file path in format 'gs://bucket-name/path/to/file.json'.
    client:             google.cloud.storage.Client. Optional. GCS storage client. If not provided, a new one will be created.
    encoding:           str. Encoding to use. Default 'utf-8'.
    **kwargs:           Additional arguments (reserved for future use).
    """

    # Parse GCS file path
    if not gcs_file_path.startswith("gs://"):
        raise ValueError(
            f"Invalid GCS file path: {gcs_file_path}. Must start with 'gs://'"
        )

    # Extract bucket name and blob path
    path_parts = gcs_file_path.replace("gs://", "").split("/", 1)
    bucket_name = path_parts[0]
    blob_path = path_parts[1] if len(path_parts) > 1 else "file.json"

    # Create client if not provided
    if client is None:
        client = storage.Client()

    # Get bucket
    bucket = client.bucket(bucket_name)

    # Get blob
    blob = bucket.blob(blob_path)

    # Convert data to JSON
    if isinstance(json_data, pd.DataFrame):
        json_str = json_data.to_json(orient="records")
    else:
        json_str = json.dumps(json_data)

    # Upload to GCS
    blob.upload_from_string(
        json_str.encode(encoding),
        content_type="application/json",
    )

    pass


def gcs_read_csv(
    gcs_file_path,
    client=None,
    **kwargs,
):
    """
    Function to read CSV file from GCS bucket.

    Parameters:

    gcs_file_path:      str. GCS file path in format 'gs://bucket-name/path/to/file.csv'.
    client:             google.cloud.storage.Client. Optional. GCS storage client. If not provided, a new one will be created.
    **kwargs:           Additional arguments to pass to pd.read_csv().

    Output:

    data:               pd.DataFrame. Data read from CSV file.
    """

    # Parse GCS file path
    if not gcs_file_path.startswith("gs://"):
        raise ValueError(
            f"Invalid GCS file path: {gcs_file_path}. Must start with 'gs://'"
        )

    # Extract bucket name and blob path
    path_parts = gcs_file_path.replace("gs://", "").split("/", 1)
    bucket_name = path_parts[0]
    blob_path = path_parts[1] if len(path_parts) > 1 else "file.csv"

    # Create client if not provided
    if client is None:
        client = storage.Client()

    # Get bucket
    bucket = client.bucket(bucket_name)

    # Get blob
    blob = bucket.blob(blob_path)

    # Download from GCS
    csv_data = blob.download_as_bytes()

    # Read CSV into DataFrame
    csv_buffer = BytesIO(csv_data)
    data = pd.read_csv(csv_buffer, **kwargs)

    return data


def gcs_read_json(
    gcs_file_path,
    client=None,
    **kwargs,
):
    """
    Function to read JSON file from GCS bucket.

    Parameters:

    gcs_file_path:      str. GCS file path in format 'gs://bucket-name/path/to/file.json'.
    client:             google.cloud.storage.Client. Optional. GCS storage client. If not provided, a new one will be created.
    **kwargs:           Additional arguments (reserved for future use).

    Output:

    data:               dict or list. Data read from JSON file.
    """

    # Parse GCS file path
    if not gcs_file_path.startswith("gs://"):
        raise ValueError(
            f"Invalid GCS file path: {gcs_file_path}. Must start with 'gs://'"
        )

    # Extract bucket name and blob path
    path_parts = gcs_file_path.replace("gs://", "").split("/", 1)
    bucket_name = path_parts[0]
    blob_path = path_parts[1] if len(path_parts) > 1 else "file.json"

    # Create client if not provided
    if client is None:
        client = storage.Client()

    # Get bucket
    bucket = client.bucket(bucket_name)

    # Get blob
    blob = bucket.blob(blob_path)

    # Download from GCS
    json_data = blob.download_as_bytes()

    # Parse JSON
    data = json.loads(json_data.decode("utf-8"))

    return data