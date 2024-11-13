from src.extract import create_s3_client
from moto import mock_aws
import pytest
import boto3

@mock_aws
def test_create_s3_client():
    s3_client=create_s3_client()

    # If we successfully created an S3 client it would have the method 'list_buckets'
    assert hasattr(s3_client, 'list_buckets')