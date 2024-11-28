import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch, MagicMock
from load import lambda_handler, S3_BUCKET
from botocore.exceptions import ClientError
import pandas as pd
import json



@mock_aws
def test_lambda_handler_unexpected_table():
    """Test lambda_handler with an unexpected table name."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=S3_BUCKET)
    test_key = "unknown_table/test.parquet"
    event = {
        "Records": [
            {
                "s3": {
                    "object": {
                        "key": test_key
                    }
                }
            }
        ]
    }

    with patch("load.create_s3_client", return_value=s3_client), \
         patch("load.connect") as mock_connect, \
         patch("load.load_parquet_from_s3", return_value=None):
        conn_mock = MagicMock()
        mock_connect.return_value = conn_mock

        # Call lambda_handler and expect ValueError
        with pytest.raises(ValueError, match="Unexpected table name: unknown_table"):
            lambda_handler(event, None)

        conn_mock.close.assert_called_once()



@mock_aws
def test_lambda_handler_general_exception():
    """Test lambda_handler when an unexpected exception occurs."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=S3_BUCKET)
    test_key = "fact_sales_order/test.parquet"
    event = {
        "Records": [
            {
                "s3": {
                    "object": {
                        "key": test_key
                    }
                }
            }
        ]
    }

    with patch("load.create_s3_client", return_value=s3_client), \
         patch("load.connect", side_effect=Exception("Database connection failed")):
        # Call lambda_handler and expect Exception
        with pytest.raises(Exception, match="Database connection failed"):
            lambda_handler(event, None)