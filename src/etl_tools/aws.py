# Import modules
import numpy as np
import pandas as pd
import json
import pickle
import awswrangler as aws
import boto3
import datetime as dt


# ============================================================================
# S3 Functions
# ============================================================================


def s3_list_objects(
    s3_bucket_name,
    s3_path,
    aws_access_key,
    aws_secret_access_key,
    region_name="us-east-1",
):
    """
    Function to list objects from S3 bucket.

    Parameters:
        s3_bucket_name: str. Name of the S3 bucket without "s3://" prefix.
        s3_path: str. Path to the file in the S3 bucket (relative to root).
        aws_access_key: str. Name of the environment variable with the AWS access key.
        aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
        region_name: str. Name of the AWS region to use.

    Returns:
        objects_list: list. List of objects in the S3 bucket.
    """

    ## Create S3 session
    my_session = boto3.Session(
        region_name=region_name,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_access_key,
    )
    ## Set up S3 resource
    s3 = my_session.client("s3")
    ## Get objects list
    objects_list = [
        obj_dict["Key"]
        for obj_dict in s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_path)[
            "Contents"
        ]
    ]
    ## Check if limit of 1000 objects was reached
    if len(objects_list) == 1000:
        ### Get paginator
        paginator = s3.get_paginator("list_objects_v2")
        ### Get pages
        pages = paginator.paginate(Bucket=s3_bucket_name, Prefix=s3_path)
        ### Iterate over pages
        objects_list = np.hstack(
            [[obj_dict["Key"] for obj_dict in page["Contents"]] for page in pages]
        )

    return objects_list


def s3_get_object(
    s3_bucket_name,
    s3_path,
    aws_access_key,
    aws_secret_access_key,
    region_name="us-east-1",
):
    """
    Function to get object from S3 bucket.

    Parameters:
        s3_bucket_name: str. Name of the S3 bucket without "s3://" prefix.
        s3_path: str. Path to the file in the S3 bucket (relative to root).
        aws_access_key: str. Name of the environment variable with the AWS access key.
        aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
        region_name: str. Name of the AWS region to use.

    Returns:
        content_object: dict. The S3 object response.
    """

    ## Create S3 session
    my_session = boto3.Session(
        region_name=region_name,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_access_key,
    )
    ## Set up S3 resource
    s3 = my_session.client("s3")
    ## Set up S3 object
    content_object = s3.get_object(Bucket=s3_bucket_name, Key=s3_path)

    return content_object


def s3_put_object(
    s3_body_content,
    s3_bucket_name,
    s3_path,
    aws_access_key,
    aws_secret_access_key,
    region_name="us-east-1",
):
    """
    Function to put object on S3 bucket.

    Parameters:
        s3_body_content: bytes. Content to be uploaded to S3.
        s3_bucket_name: str. Name of the S3 bucket without "s3://" prefix.
        s3_path: str. Path to the file in the S3 bucket (relative to root).
        aws_access_key: str. Name of the environment variable with the AWS access key.
        aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
        region_name: str. Name of the AWS region to use.
    """

    ## Create S3 session
    my_session = boto3.Session(
        region_name=region_name,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_access_key,
    )
    ## Set up S3 resource
    s3 = my_session.client("s3")
    ## Set up S3 object
    s3.put_object(Body=s3_body_content, Bucket=s3_bucket_name, Key=s3_path)

    pass


def s3_read_file(
    s3_bucket_name,
    s3_path,
    aws_access_key,
    aws_secret_access_key,
    region_name="us-east-1",
    encoding="utf-8",
    file_type="plain",
):
    """
    Function to read .csv or .json file from S3 bucket.

    Parameters:
        s3_bucket_name: str. Name of the S3 bucket without "s3://" prefix.
        s3_path: str. Path to the file in the S3 bucket (relative to root).
        aws_access_key: str. Name of the environment variable with the AWS access key.
        aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
        region_name: str. Name of the AWS region to use.
        encoding: str. Encoding to use for reading the file.
        file_type: str. Type of file to read ("csv" or "plain"). Default "plain".

    Returns:
        file_content: BytesIO or str. Content of the file.
    """

    # Get content object
    content_object = s3_get_object(
        s3_bucket_name,
        s3_path,
        aws_access_key,
        aws_secret_access_key,
        region_name=region_name,
    )
    if file_type == "csv":
        ### Get and decode content
        file_content = content_object.get("Body")
    elif file_type == "plain":
        ### Get and decode content
        file_content = content_object.get("Body").read().decode(encoding)

    return file_content


def s3_read_json(
    s3_bucket_name,
    s3_path,
    aws_access_key,
    aws_secret_access_key,
    region_name="us-east-1",
    encoding="utf-8",
):
    """
    Function to read json from S3 bucket.

    Parameters:
        s3_bucket_name: str. Name of the S3 bucket without "s3://" prefix.
        s3_path: str. Path to the json file in the S3 bucket (relative to root).
        aws_access_key: str. Name of the environment variable with the AWS access key.
        aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
        region_name: str. Name of the AWS region to use.
        encoding: str. Encoding to use for reading the file.

    Returns:
        json_content: dict. JSON content from the file.
    """

    # Read S3 file
    file_content = s3_read_file(
        s3_bucket_name,
        s3_path,
        aws_access_key,
        aws_secret_access_key,
        region_name=region_name,
        encoding=encoding,
        file_type="plain",
    )
    ## Read json
    json_content = json.loads(file_content)

    return json_content


def s3_write_json(
    json_data,
    s3_bucket_name,
    s3_path,
    aws_access_key,
    aws_secret_access_key,
    region_name="us-east-1",
    encoding="utf-8",
):
    """
    Function to write json to S3 bucket.

    Parameters:
        json_data: dict. Data to be written to json file.
        s3_bucket_name: str. Name of the S3 bucket without "s3://" prefix.
        s3_path: str. Path to the json file in the S3 bucket (relative to root).
        aws_access_key: str. Name of the environment variable with the AWS access key.
        aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
        region_name: str. Name of the AWS region to use.
        encoding: str. Encoding to use for writing the file.
    """

    # Set and encode data
    s3_body_content = bytes(json.dumps(json_data).encode(encoding))
    ## Upload content to S3
    s3_put_object(
        s3_body_content,
        s3_bucket_name,
        s3_path,
        aws_access_key,
        aws_secret_access_key,
        region_name=region_name,
    )

    pass


def s3_read_csv(
    s3_bucket_name,
    s3_path,
    aws_access_key,
    aws_secret_access_key,
    region_name="us-east-1",
    **kwargs,
):
    """
    Function to read csv from S3 bucket.

    Parameters:
        s3_bucket_name: str. Name of the S3 bucket without "s3://" prefix.
        s3_path: str. Path to the csv file in the S3 bucket (relative to root).
        aws_access_key: str. Name of the environment variable with the AWS access key.
        aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
        region_name: str. Name of the AWS region to use.
        **kwargs: Additional keyword arguments to pass to pd.read_csv.

    Returns:
        csv_content: pd.DataFrame. CSV content as a DataFrame.
    """

    # Read S3 file
    file_content = s3_read_file(
        s3_bucket_name,
        s3_path,
        aws_access_key,
        aws_secret_access_key,
        region_name=region_name,
        encoding="utf-8",
        file_type="csv",
    )
    ## Read csv
    csv_content = pd.read_csv(file_content, **kwargs)

    return csv_content


def s3_write_parquet(
    data,
    s3_bucket_name,
    s3_path,
    aws_access_key,
    aws_secret_access_key,
    region_name="us-east-1",
):
    """
    Function to write DataFrame to .parquet in S3 bucket.

    Parameters:
        data: pd.DataFrame. Data to be written to .parquet file.
        s3_bucket_name: str. Name of the S3 bucket without "s3://" prefix.
        s3_path: str. Path to the .parquet file in the S3 bucket (relative to root).
        aws_access_key: str. Name of the environment variable with the AWS access key.
        aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
        region_name: str. Name of the AWS region to use.
    """

    # Set and encode data
    s3_body_content = data.to_parquet()
    ## Upload content to S3
    s3_put_object(
        s3_body_content,
        s3_bucket_name,
        s3_path,
        aws_access_key,
        aws_secret_access_key,
        region_name=region_name,
    )
    pass


def s3_read_pkl(
    s3_bucket_name,
    s3_pickle_path,
    aws_access_key,
    aws_secret_access_key,
    region_name="us-east-1",
):
    """
    Function to read pickle file from S3.

    Parameters:
        s3_bucket_name: str. Name of the S3 bucket without "s3://" prefix.
        s3_pickle_path: str. Path to the pickle file in the S3 bucket (relative to bucket).
        aws_access_key: str. Name of the environment variable with the AWS access key.
        aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
        region_name: str. Name of the AWS region to use.

    Returns:
        pickle_object: object. Unpickled Python object.
    """
    # Download from S3
    pkl_file = (
        s3_get_object(
            s3_bucket_name,
            s3_pickle_path,
            aws_access_key,
            aws_secret_access_key,
            region_name=region_name,
        )
        .get("Body")
        .read()
    )
    ## Load threshold for center
    pickle_object = pickle.loads(pkl_file)

    return pickle_object


def s3_upload_csv(
    data,
    s3_file_path,
    aws_access_key,
    aws_secret_access_key,
    region_name="us-east-1",
    sep=",",
    index=False,
    encoding="utf-8",
):
    """
    Function to upload data as CSV to S3 bucket.

    Parameters:
        data: pd.DataFrame. Data to upload.
        s3_file_path: str. S3 file path.
        aws_access_key: str. Name of the environment variable with the AWS access key.
        aws_secret_access_key: str. Name of the environment variable with the AWS secret access key.
        region_name: str. Name of the AWS region to use.
        sep: str. Separator to use for CSV data.
        index: bool. Whether to include the index in the file.
        encoding: str. Encoding to use.
    """

    # Create S3 session
    my_session = boto3.Session(
        region_name=region_name,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_access_key,
    )
    ## Upload data to S3 bucket
    aws.s3.to_csv(
        data,
        path=s3_file_path,
        sep=sep,
        my_session=my_session,
        index=index,
        encoding=encoding,
    )

    pass


# ============================================================================
# DynamoDB Functions
# ============================================================================


def dynamodb_read_data(
    table_name, aws_access_key_id, aws_secret_access_key, region_name, **kwargs
):
    """
    Function to read data from DynamoDB.

    Parameters:
        table_name: str. Name of the DynamoDB table.
        aws_access_key_id: str. AWS access key ID.
        aws_secret_access_key: str. AWS secret access key.
        region_name: str. AWS region name.
        **kwargs: Additional arguments for DynamoDB scan operations.

    Returns:
        data: list. List of items from the DynamoDB table.
    """

    # Set resource
    dynamodb = boto3.resource(
        "dynamodb",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )
    ## Evaluate keyword arguments
    kwargs_eval = {key: eval(val) for key, val in kwargs.items()}
    ## Get table
    table = dynamodb.Table(table_name)
    ## Scan table
    scan_response = table.scan(**kwargs_eval)
    ## Get data from table
    data = scan_response["Items"]
    while "LastEvaluatedKey" in scan_response:  ### Iterate over each scan response
        ### Scan table
        scan_response = table.scan(
            ExclusiveStartKey=scan_response["LastEvaluatedKey"], **kwargs_eval
        )
        ### Add data
        data.extend(scan_response["Items"])

    return data


def dynamodb_upload_data(
    data,
    table_name,
    aws_access_key_id,
    aws_secret_access_key,
    region_name,
    **kwargs,
):
    """
    Function to upload data to DynamoDB.

    Parameters:
        data: pd.DataFrame or list. Data to upload.
        table_name: str. Name of the DynamoDB table.
        aws_access_key_id: str. AWS access key ID.
        aws_secret_access_key: str. AWS secret access key.
        region_name: str. AWS region name.
        **kwargs: Additional arguments for DynamoDB operations.
    """

    # Set resource
    dynamodb = boto3.resource(
        "dynamodb",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )
    ## Get table
    table = dynamodb.Table(table_name)

    ## Convert DataFrame to list of dictionaries if needed
    if isinstance(data, pd.DataFrame):
        items = data.to_dict(orient="records")
    else:
        items = data if isinstance(data, list) else [data]

    ## Get batch size (default 25, maximum for DynamoDB)
    batch_size = kwargs.get("batch_size", 25)

    ## Upload data in batches
    with table.batch_writer(
        batch_size=batch_size,
        overwrite_by_pkeys=kwargs.get("overwrite_by_pkeys", ["id"]),
    ) as batch:
        ### Upload each item
        for item in items:
            batch.put_item(Item=item)

    pass
