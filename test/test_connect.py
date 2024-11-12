from src.util_functions import connect
import pytest
from unittest.mock import patch, MagicMock
from pg8000.native import Connection
import os

# Create mock credentials
@patch.dict(os.environ, {
    'user':'test_user',
    'database':'test_db',
    'password':'test_pass',
    'host':'localhost',
    'port': '5432'
})

# Patch the Connection to use mock connection / credentials
@patch('src.util_functions.Connection')
def test_connection_success(mock_connection):

    # Create the mock connection
    mock_conn_instance=MagicMock()
    mock_connection.return_value = mock_conn_instance

    # Connect to mock connection with mock credentials
    conn = connect()
    
    assert conn == mock_conn_instance

    # Check mock credentials were used
    mock_connection.assert_called_once_with(
        user='test_user',
        database='test_db',
        password='test_pass',
        host='localhost',
        port='5432'
    )

    # Test for missing environment variables
    # Test for invalid environment variables (wrong username etc)
    # Test database not available / not connecting
    # Any others?