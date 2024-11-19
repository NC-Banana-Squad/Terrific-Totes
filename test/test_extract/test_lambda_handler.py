from extract import lambda_handler
from moto import mock_aws
from unittest.mock import patch, MagicMock
from botocore.exceptions import NoCredentialsError, ClientError
from pg8000.exceptions import InterfaceError, DatabaseError
import pytest
import os
import boto3
import botocore
import unittest


@pytest.fixture
def set_env_vars():
    with patch.dict(
        os.environ,
        {
            "user": "test_user",
            "database": "test_db",
            "password": "test_pass",
            "host": "localhost",
            "port": "5432",
        },
    ):
        yield


# def xtest_lambda_handler_no_aws_credentials():
#     with patch("src.extract.util_functions.create_s3_client") as mock_create_s3_client:
#         mock_create_s3_client.side_effect = NoCredentialsError
#         response = lambda_handler({}, {})
#         assert response == {
#             "result": "Failure",
#             "error": "AWS credentials not found. Unable to create S3 client",
#         }


# def xtest_lambda_handler_s3_client_error(set_env_vars):
#     with patch("src.extract.util_functions.create_s3_client") as mock_create_s3_client:
#         mock_s3_client = MagicMock()

#         mock_s3_client.list_objects.side_effect = ClientError({}, "ListObjects")

#         mock_create_s3_client.return_value = mock_s3_client
#         response = lambda_handler({}, {})
#         assert response == {"result": "Failure", "error": "Error creating S3 client"}

# Can be used as a template for Lambda Handler - not required here as 'initial extract' shouldn't be worrying about client creation failure

# @patch("src.extract.extract.connect")
# @patch("src.extract.extract.create_s3_client")
# def test_create_s3_client_failure(self, mock_create_s3_client, mock_connect):
#     mock_create_s3_client.side_effect = Exception("S3 client creation error")
#     print("dupa")
#     with patch("src.extract.extract.logging.error") as mock_error:
#         result = initial_extract(mock_create_s3_client, mock_connect)
#     print("dupa1")
#     mock_error.assert_called_with(
#         "Failed to create a client from create_client function: S3 client creation error"
#     )
#     print("dupa2")
#     assert result == {
#         "result": "Failed to create an object in banana-squad-ingested-data bucket"
#     }

#     assert not mock_connect.called
