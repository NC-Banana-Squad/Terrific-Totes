from util_functions import store_in_s3
from moto import mock_aws
from io import StringIO
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import pytest
from unittest.mock import patch, MagicMock


@mock_aws
def test_store_in_s3_succesfully_uploads():
    # Set up mock s3:
    bucket_name = "test-bucket"
    file_name = "test-file.csv"
    csv_content = "column1,column2\nvalue1,value2"
    csv_buffer = StringIO(csv_content)

    # Initialise mock s3 client:
    mock_s3_client = boto3.client("s3", region_name="us-east-1")
    mock_s3_client.create_bucket(Bucket=bucket_name)

    # Call the function
    store_in_s3(mock_s3_client, csv_buffer, bucket_name, file_name)

    response = mock_s3_client.get_object(Bucket=bucket_name, Key=file_name)
    content = response["Body"].read().decode("utf-8")

    assert content == csv_content


@mock_aws
def test_store_in_s3_no_such_bucket():
    # Set up mock s3:
    bucket_name = "invalid-bucket"
    file_name = "test-file.csv"
    csv_content = "column1,column2\nvalue1,value2"
    csv_buffer = StringIO(csv_content)

    # Initialise mock s3 client:
    mock_s3_client = boto3.client("s3", region_name="us-east-1")

    with pytest.raises(ClientError) as err_info:
        # Call the function
        store_in_s3(mock_s3_client, csv_buffer, bucket_name, file_name)

    assert err_info.value.response["Error"]["Code"] == "NoSuchBucket"


@mock_aws
def test_store_in_s3_access_denied():
    mock_s3_client = MagicMock()
    csv_buffer = StringIO()
    csv_buffer.write("test,data\n1,2")

    error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
    mock_s3_client.put_object.side_effect = ClientError(error_response, "PutObject")

    with pytest.raises(ClientError) as exc_info:
        store_in_s3(mock_s3_client, csv_buffer, "test-bucket", "test.csv")

    assert exc_info.value.response["Error"]["Code"] == "AccessDenied"
    mock_s3_client.put_object.assert_called_once_with(
        Body=csv_buffer.getvalue(), Bucket="test-bucket", Key="test.csv"
    )
