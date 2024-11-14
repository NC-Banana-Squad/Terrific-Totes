from src.extract import lambda_handler
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
    with patch.dict(os.environ, {
        'user': 'test_user',
        'database': 'test_db',
        'password': 'test_pass',
        'host': 'localhost',
        'port': '5432'
    }):
        yield

def test_lambda_handler_no_aws_credentials():
    with patch('src.extract.create_s3_client') as mock_create_s3_client:
        mock_create_s3_client.side_effect = NoCredentialsError
        response = lambda_handler({},{})
        assert response == {"result": "Failure",
                            "error": "AWS credentials not found. Unable to create S3 client"}

def test_lambda_handler_s3_client_error(set_env_vars):
    with patch('src.extract.create_s3_client') as mock_create_s3_client:
        mock_s3_client = MagicMock()

        mock_s3_client.list_objects.side_effect = ClientError({},'ListObjects')

        mock_create_s3_client.return_value = mock_s3_client
        response = lambda_handler({},{})
        assert response == {"result": "Failure",
                            "error": "Error creating S3 client"}