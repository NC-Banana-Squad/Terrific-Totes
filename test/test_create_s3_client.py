from src.extract import create_s3_client
from moto import mock_aws
from unittest.mock import patch
from botocore.exceptions import NoCredentialsError
import pytest
import boto3

@mock_aws
def test_create_s3_client():
    s3_client=create_s3_client()

    # Successfull creation of S3 client would have the method 'list_buckets'
    assert hasattr(s3_client, 'list_buckets')

@patch('src.extract.boto3.client')
def test_create_s3_no_credentials(mock_boto_client, capsys):

    # Simulates an error caused by no credentials being provided
    mock_boto_client.side_effect = NoCredentialsError()

    s3_client = create_s3_client()

    # Captures printed output
    captured = capsys.readouterr()

    # Assert None is returned when error occurs
    assert s3_client is None
    assert "Error: AWS credentials not found." in captured.out



