import pytest
from unittest.mock import MagicMock, patch
from extract import continuous_extract
from datetime import datetime

"""
Tests for the continuous_extract function.

This module includes tests to validate the behavior of the continuous_extract 
function under various scenarios, such as successful extraction, no new data, 
database errors, and multiple table extractions.

Fixtures:
    - `mock_data`: Provides mock data used across tests.
    - `mock_s3_client`: Mocks interactions with S3.
    - `mock_db_connection`: Mocks interactions with the database.

Each test uses mocked dependencies to isolate the functionality of 
`continuous_extract` and ensures proper behavior of individual components.

Modules tested:
    - extract.continuous_extract
"""

@pytest.fixture
def mock_data():
    """Provide mock data for the tests."""
    return {
        "mock_table_data": [("table1",), ("table2",)],
        "mock_rows": [
            {"id": 1, "name": "Test", "created_at": datetime(2024, 1, 1)},
            {"id": 2, "name": "Test2", "created_at": datetime(2024, 1, 2)},
        ],
        "mock_columns": [{"name": "id"}, {"name": "name"}, {"name": "created_at"}],
        "last_extracted_datetime": "2024-01-01T00:00:00",
    }

@pytest.fixture
def mock_s3_client(mock_data):
    """Mock the S3 client."""
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        "Body": MagicMock(
            read=MagicMock(return_value=mock_data["last_extracted_datetime"].encode())
        )
    }
    return mock_s3

@pytest.fixture
def mock_db_connection(mock_data):
    """Mock the database connection."""
    mock_conn = MagicMock()
    # Remove the default side_effect from the fixture
    mock_conn.columns = mock_data["mock_columns"]
    return mock_conn

@patch("extract.create_file_name", return_value="mocked_file_name.csv")
@patch("extract.format_to_csv", return_value="mocked_csv_data")
@patch("extract.store_in_s3")
def test_continuous_extract_successful(
    mock_store_in_s3, 
    mock_format_to_csv, 
    mock_create_file_name, 
    mock_s3_client, 
    mock_db_connection, 
    mock_data
):
    """Test successful extraction and storage of new data."""
    # Set up side_effect specifically for this test
    mock_db_connection.run.side_effect = [
        [("table1",)],  # First call - get table names (just one table for simplicity)
        mock_data["mock_rows"]  # Second call - get rows for table1
    ]

    result = continuous_extract(mock_s3_client, mock_db_connection)

    # Add debug print to see actual calls
    print("\nActual calls:")
    for call in mock_db_connection.run.mock_calls:
        print(f"Call: {call}")

    # Verify database queries were made with more flexible datetime comparison
    assert mock_db_connection.run.call_count == 2
    first_call = mock_db_connection.run.mock_calls[0]
    second_call = mock_db_connection.run.mock_calls[1]
    
    # Verify first call (getting table names)
    assert first_call.args[0] == "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name != '_prisma_migrations'"
    
    # Verify second call (getting rows) starts and ends correctly
    second_call_query = second_call.args[0]
    assert second_call_query.startswith("SELECT * FROM table1 WHERE created_at > '")
    assert second_call_query.endswith("'")
    
    # Verify the rest of the process
    mock_create_file_name.assert_called_with("table1")
    mock_format_to_csv.assert_called_with(
        mock_data["mock_rows"], 
        ["id", "name", "created_at"]
    )
    mock_store_in_s3.assert_called_with(
        mock_s3_client,
        "mocked_csv_data",
        "banana-squad-ingested-data",
        "mocked_file_name.csv"
    )

    assert result == ["mocked_file_name.csv"]

@patch("extract.create_file_name", return_value="mocked_file_name.csv")
@patch("extract.store_in_s3")
def test_continuous_extract_no_new_data(
    mock_store_in_s3,
    mock_create_file_name,
    mock_s3_client,
    mock_db_connection,
    mock_data
):
    """Test when no new data is available since last extraction."""
    # Set up side_effect specifically for this test
    mock_db_connection.run.side_effect = [
        [("table1",)],  # First call - get table names (just one table)
        []  # Second call - no new rows
    ]

    result = continuous_extract(mock_s3_client, mock_db_connection)

    assert mock_db_connection.run.call_count == 2
    mock_create_file_name.assert_called_once_with("table1")
    mock_store_in_s3.assert_not_called()
    assert result == []

@patch("extract.create_file_name", return_value="mocked_file_name.csv")
@patch("extract.store_in_s3")
def test_continuous_extract_database_error(
    mock_store_in_s3,
    mock_create_file_name,
    mock_s3_client,
    mock_db_connection,
    mock_data
):
    """Test handling of database query errors."""
    mock_db_connection.run.side_effect = [
        [("table1",)],  # First call - get table names
        Exception("Database error")  # Second call - raise exception
    ]

    with pytest.raises(Exception, match="Database error"):
        continuous_extract(mock_s3_client, mock_db_connection)

    mock_create_file_name.assert_called_once_with("table1")
    mock_store_in_s3.assert_not_called()

@patch("extract.create_file_name", return_value="mocked_file_name.csv")
@patch("extract.format_to_csv", return_value="mocked_csv_data")
@patch("extract.store_in_s3")
def test_continuous_extract_multiple_tables(
    mock_store_in_s3,
    mock_format_to_csv,
    mock_create_file_name,
    mock_s3_client,
    mock_db_connection,
    mock_data
):
    """Test extraction from multiple tables with new data."""
    mock_db_connection.run.side_effect = [
        [("table1",), ("table2",)],  # First call - get table names (two tables)
        mock_data["mock_rows"],      # Second call - rows for table1
        mock_data["mock_rows"]       # Third call - rows for table2
    ]

    result = continuous_extract(mock_s3_client, mock_db_connection)

    assert mock_db_connection.run.call_count == 3
    assert mock_create_file_name.call_count == 2
    assert mock_format_to_csv.call_count == 2
    assert mock_store_in_s3.call_count == 2
    assert result == ["mocked_file_name.csv", "mocked_file_name.csv"]
