import pytest
from unittest.mock import patch, MagicMock
import boto3
from moto import mock_aws
import pandas as pd
import json
import io
from transform import lambda_handler, get_data_frame

@pytest.fixture
def s3_mock():
    with mock_aws():
        s3_client = boto3.client("s3", region_name="eu-west-2")
        s3_client.create_bucket(
            Bucket="banana-squad-ingested-data",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
        )
        s3_client.create_bucket(
            Bucket="banana-squad-processed-data",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
        )
        yield s3_client

@pytest.fixture
def fake_event():
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "banana-squad-ingested-data"},
                    "object": {"key": "updated_tables.json"},
                }
            }
        ]
    }

@pytest.fixture
def mock_transform_utils():
    with patch("transform.fact_sales_order", return_value=pd.DataFrame({"col": [1]})) as fact_sales_order:
        with patch("transform.dim_date", return_value=pd.DataFrame({"date_col": [1]})) as dim_date:
            with patch("transform.dim_counterparty", return_value=pd.DataFrame({"col": [2]})) as dim_counterparty:
                yield {
                    "fact_sales_order": fact_sales_order,
                    "dim_date": dim_date,
                    "dim_counterparty": dim_counterparty,
                }

def test_get_data_frame_success(s3_mock):
    s3_mock.put_object(
        Bucket="banana-squad-ingested-data",
        Key="sales_order.csv",
        Body="col1,col2\n1,2\n3,4",
    )
    df = get_data_frame(s3_mock, "banana-squad-ingested-data", "sales_order.csv")
    assert not df.empty
    assert df.shape == (2, 2)

def test_get_data_frame_failure(s3_mock):
    df = get_data_frame(s3_mock, "banana-squad-ingested-data", "nonexistent.csv")
    assert df is None

def test_lambda_handler_missing_table(s3_mock, fake_event):
    data = {"updated_tables": ["nonexistent_table.csv"]}
    s3_mock.put_object(
        Bucket="banana-squad-ingested-data",
        Key="updated_tables.json",
        Body=json.dumps(data),
    )
    response = lambda_handler(fake_event, None)
    assert response == "Transformation completed"