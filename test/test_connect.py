# from src.util_functions import connect
from src.extract import connect
from unittest.mock import patch, MagicMock
from pg8000.native import Error
from pg8000.exceptions import InterfaceError, DatabaseError
import os
import pytest

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

@patch('src.extract.Connection')
def test_connection_success(mock_connection, set_env_vars):

    # Create the mock connection and connect to it
    mock_conn_instance=MagicMock()
    mock_connection.return_value = mock_conn_instance

    conn = connect()
        
    assert conn == mock_conn_instance

    # Check mock credentials were used (once)
    # conn = connect() # uncomment this to cause test to fail
    mock_connection.assert_called_once_with(
        user='test_user',
        database='test_db',
        password='test_pass',
        host='localhost',
        port='5432'
    )

def test_handles_missing_env_variables():
    with pytest.raises(KeyError):
        connect()

@patch('src.extract.Connection', side_effect=InterfaceError("Invalid credentials"))
def test_handles_interface_errors(mock_connection, set_env_vars):
    with pytest.raises(InterfaceError):
        connect()

@patch('src.extract.Connection', side_effect=DatabaseError("Database connection failed"))
def test_handles_database_errors(mock_connection, set_env_vars):
    with pytest.raises(DatabaseError):
        connect()

@patch('src.extract.Connection', side_effect=Error("General pg8000 error"))
def test_handles_general_errors(mock_connection, set_env_vars):
    with pytest.raises(Error):
        connect()

@patch('src.extract.Connection', side_effect=Exception("General exception"))
def test_handles_general_exceptions(mock_connection, set_env_vars):
    with pytest.raises(Exception):
        connect()