from extract import lambda_handler, continuous_extract, initial_extract

# from util_functions import (
#     connect,
#     create_s3_client,
#     create_file_name,
#     format_to_csv,
#     store_in_s3,
# )
from moto import mock_aws
from botocore.exceptions import NoCredentialsError, ClientError
from pg8000.exceptions import InterfaceError, DatabaseError
from unittest.mock import patch, MagicMock
from datetime import datetime
import unittest
import pytest
import boto3


@mock_aws
@patch("extract.create_s3_client")
@patch("extract.connect")
@patch("extract.initial_extract")
@patch("extract.continuous_extract")
def test_continuous_extract_called_when_last_extracted_exists(
    mock_continuous, mock_initial, mock_connect, mock_s3_client
):
    # Setup mock Secrets Manager
    secret_name = "database_credentials"
    region_name = "eu-west-2"
    client = boto3.client("secretsmanager", region_name=region_name)
    client.create_secret(
        Name=secret_name,
        SecretString='{"user":"test_user","database":"test_db","password":"test_pass","host":"localhost","port":"5432"}',
    )

    # Mock S3 client
    mock_s3 = MagicMock()
    mock_s3.list_objects.return_value = {"Contents": [{"Key": "last_extracted.txt"}]}
    mock_s3_client.return_value = mock_s3

    # Mock database connection
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # Call lambda_handler
    result = lambda_handler({}, {})

    # Assert continuous_extract was called
    mock_continuous.assert_called_once_with(mock_s3, mock_conn)
    mock_initial.assert_not_called()

    # Assert the result is success
    assert result["result"] == "Success"


@mock_aws
@patch("extract.connect")
@patch("extract.create_s3_client")
@patch("extract.initial_extract")
@patch("extract.continuous_extract")
def test_initial_extract_called_when_last_extracted_missing(
    mock_continuous, mock_initial, mock_s3_client, mock_connect
):
    # Mock S3 client
    mock_s3 = MagicMock()
    mock_s3.list_objects.return_value = {}  # Simulate missing file
    mock_s3_client.return_value = mock_s3

    # Mock database connection
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # Call lambda_handler
    result = lambda_handler({}, {})

    # Assert initial_extract was called
    mock_initial.assert_called_once_with(mock_s3, mock_conn)
    mock_continuous.assert_not_called()

    # Assert the result is success
    assert result["result"] == "Success"
