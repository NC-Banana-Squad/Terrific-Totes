import pytest
from util_functions import connect
from unittest.mock import patch, MagicMock


def test_connect():

    mock_secret = {
        "user": "test_user",
        "database": "test_db",
        "password": "test_password",
        "host": "test_host",
        "port": 5432,
    }

    mock_connection = MagicMock()

    with patch(
        "util_functions.get_secret", return_value=mock_secret
    ) as mock_get_secret, patch(
        "util_functions.Connection", return_value=mock_connection
    ) as mock_connection_cls:
        connection = connect()

        mock_get_secret.assert_called_once_with("database_credentials")
        mock_connection_cls.assert_called_once_with(
            user="test_user",
            database="test_db",
            password="test_password",
            host="test_host",
            port=5432,
        )

        assert connection == mock_connection


import pytest


def test_connect_missing_key():

    mock_secret = {
        "user": "test_user",
        "database": "test_db",
    }

    with patch("util_functions.get_secret", return_value=mock_secret):
        with pytest.raises(KeyError, match="password"):
            connect()
