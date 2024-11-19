# from src.util_functions import connect
from util_functions import connect
from unittest.mock import patch, MagicMock
from pg8000.native import Error
from pg8000.exceptions import InterfaceError, DatabaseError
import os
import pytest


@pytest.fixture
def xset_env_vars():
    with patch.dict(
        os.environ,
        {
            "user": "test_user",
            "database": "test_db",
            "password": "test_pass",
            "host": "localhost",
            "port": "5432",
        },
    ):
        yield


@patch("src.extract.util_functions.pg8000.Connection")
@patch("src.extract.util_functions.dotenv.load_dotenv")
def xtest_connection_success(mock_dotenv, mock_connection):
    os.environ["user"] = "test_user"
    os.environ["database"] = "test_db"
    os.environ["password"] = "test_pass"
    os.environ["host"] = "localhost"
    os.environ["port"] = "5432"
    mock_conn_instance = MagicMock()
    mock_connection.return_value = mock_conn_instance

    conn = connect()

    assert conn == mock_conn_instance
    mock_dotenv.assert_called_once()
    mock_connection.assert_called_once_with(
        user="test_user",
        database="test_db",
        password="test_pass",
        host="localhost",
        port="5432",
    )


def xtest_handles_missing_env_variables():
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(KeyError):
            connect()


@patch(
    "src.extract.util_functions.connect",
    side_effect=InterfaceError("Invalid credentials"),
)
def xtest_handles_interface_errors(mock_connection, set_env_vars):
    with pytest.raises(InterfaceError):
        connect()


@patch(
    "src.extract.util_functions.pg8000.Connection",
    side_effect=DatabaseError("Database connection failed"),
)
def xtest_handles_database_errors(mock_connection, set_env_vars):
    with pytest.raises(DatabaseError):
        connect()


@patch("src.extract.util_functions.connect", side_effect=Error("General pg8000 error"))
def xtest_handles_general_errors(mock_connection, set_env_vars):
    with pytest.raises(Error):
        connect()


@patch("src.extract.util_functions.connect", side_effect=Exception("General exception"))
def xtest_handles_general_exceptions(mock_connection, set_env_vars):
    with pytest.raises(Exception):
        connect()
