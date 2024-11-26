import pytest
from unittest.mock import MagicMock, patch
from extract import initial_extract


@pytest.fixture
def mock_data():
    """Provide mock data for the tests."""
    return {
        "mock_table_data": [("table1",)],
        "mock_rows": [
            {"id": 1, "name": "Test", "created_at": "2024-01-01 00:00:00"},
            {"id": 2, "name": "Test2", "created_at": "2024-01-02 00:00:00"},
        ],
        "mock_columns": [{"name": "id"}, {"name": "name"}, {"name": "created_at"}],
    }


@pytest.fixture
def mock_db_connection(mock_data):
    """Mock the database connection."""
    mock_conn = MagicMock()
    mock_conn.run.side_effect = [
        mock_data["mock_table_data"],  # Response for table names query
        mock_data["mock_rows"],  # Response for data query
    ]
    mock_conn.columns = mock_data["mock_columns"]
    return mock_conn


@pytest.fixture
def mock_s3_client():
    """Mock the S3 client."""
    return MagicMock()


@patch("extract.create_file_name", return_value="mocked_file_name.csv")
@patch("extract.format_to_csv", return_value="mocked_csv_data")
@patch("extract.store_in_s3")
def test_initial_extract_successful_extraction(
    mock_store_in_s3, mock_format_to_csv, mock_create_file_name, mock_s3_client, mock_db_connection, mock_data
):
    # Mock the store_in_s3 function
    mock_store_in_s3.return_value = True

    # Call the function to test
    result = initial_extract(mock_s3_client, mock_db_connection)

    # Assertions
    assert result == ["mocked_file_name.csv"]
    mock_create_file_name.assert_called_once_with("table1")
    mock_format_to_csv.assert_called_once_with(mock_data["mock_rows"], ["id", "name", "created_at"])
    mock_store_in_s3.assert_called_once_with(
        mock_s3_client, "mocked_csv_data", "banana-squad-ingested-data", "mocked_file_name.csv"
    )


@patch("extract.create_file_name", return_value="mocked_file_name.csv")
@patch("extract.store_in_s3", side_effect=Exception("S3 upload failed"))
def test_initial_extract_s3_upload_failure(
    mock_store_in_s3, mock_create_file_name, mock_s3_client, mock_db_connection
):
    with pytest.raises(Exception, match="S3 upload failed"):
        initial_extract(mock_s3_client, mock_db_connection)
    mock_store_in_s3.assert_called_once()


@patch("extract.create_file_name", return_value="mocked_file_name.csv")
@patch("extract.format_to_csv", return_value="mocked_csv_data")
@patch("extract.store_in_s3")
def test_initial_extract_no_rows_in_table(
    mock_store_in_s3, mock_format_to_csv, mock_create_file_name, mock_s3_client, mock_db_connection, mock_data
):
    # Modify mock to return empty rows
    mock_db_connection.run.side_effect = [
        mock_data["mock_table_data"],  # Table names
        [],  # No rows for the table
    ]

    result = initial_extract(mock_s3_client, mock_db_connection)

    assert result == []
    mock_create_file_name.assert_called_once_with("table1")
    mock_format_to_csv.assert_not_called()
    mock_store_in_s3.assert_not_called()


@patch("extract.create_file_name", return_value="mocked_file_name.csv")
@patch("extract.format_to_csv", return_value="mocked_csv_data")
@patch("extract.store_in_s3")
def test_initial_extract_multiple_tables(
    mock_store_in_s3, mock_format_to_csv, mock_create_file_name, mock_s3_client, mock_db_connection, mock_data
):
    # Modify mock to return multiple tables
    mock_data["mock_table_data"] = [("table1",), ("table2",)]
    mock_db_connection.run.side_effect = [
        mock_data["mock_table_data"],  # Table names
        mock_data["mock_rows"],  # Rows for table1
        mock_data["mock_rows"],  # Rows for table2
    ]

    result = initial_extract(mock_s3_client, mock_db_connection)

    assert result == ["mocked_file_name.csv", "mocked_file_name.csv"]
    assert mock_create_file_name.call_count == 2
    assert mock_format_to_csv.call_count == 2
    assert mock_store_in_s3.call_count == 2
