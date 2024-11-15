import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch, call
from src.extract.extract import continuous_extract
from src.extract.util_functions import create_file_name, format_to_csv, store_in_s3


# Retrieving the last extracted timestamp from the last_extracted.txt file
def test_continuous_extract_retrieves_last_extracted_timestamp():
    pass


# Formatting
@pytest.fixture
def s3_resource():
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="banana-squad-code")
        s3_client.put_object(Body=b"2024-01-01")
        s3_client.get_object("banana-squad-code", "last_extracted.txt")
        yield s3_client


@pytest.fixture
def db_connection():
    with patch("src.extract.util_functions.connect") as mock_connect:
        mock_db = mock_connect.return_value
        mock_db.run.side_effect = [
            [{"table_name": "users"}, {"table_name": "orders"}],
            [{"id": 1, "created_at": "2024-01-02"}],
            [{"id": 2, "created_at": "2024-01-02"}],
        ]
        mock_db.columns = [{"name": "id"}, {"name": "created_at"}]
        yield mock_db


def test_continuous_extract_successful_flow(s3_resource, db_connection):
    with patch(
        "src.extract.util_functions.create_file_name"
    ) as mock_create_file_name, patch(
        "src.extract.util_functions.format_to_csv"
    ) as mock_format_to_csv, patch(
        "src.extract.util_functions.store_in_s3"
    ) as mock_store_in_s3:
        mock_create_file_name.side_effect = (
            lambda table: f"{table['table_name']}_export.csv"
        )
        mock_format_to_csv.return_value = "csv_data"

        result = continuous_extract()

        assert result == {"result": "Success"}

        # Verify S3 interactions
        s3_resource.Object(
            "banana-squad-code", "last_extracted.txt"
        ).get.assert_called_once()

        # Verify database queries
        expected_db_calls = [
            call(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name != '_prisma_migrations'"
            ),
            call("SELECT * FROM users WHERE created_at > 2024-01-01"),
            call("SELECT * FROM orders WHERE created_at > 2024-01-01"),
        ]
        assert db_connection.run.call_args_list == expected_db_calls

        # Verify file creation and storage
        assert mock_create_file_name.call_args_list == [
            call({"table_name": "users"}),
            call({"table_name": "orders"}),
        ]
        assert mock_format_to_csv.call_args_list == [
            call(
                [{"id": 1, "created_at": "2024-01-02"}],
                [{"name": "id"}, {"name": "created_at"}],
            ),
            call(
                [{"id": 2, "created_at": "2024-01-02"}],
                [{"name": "id"}, {"name": "created_at"}],
            ),
        ]
        assert mock_store_in_s3.call_args_list == [
            call(s3_resource, "csv_data", "banana-squad-code", "users_export.csv"),
            call(s3_resource, "csv_data", "banana-squad-code", "orders_export.csv"),
        ]

        # Verify connection was closed
        db_connection.close.assert_called_once()
