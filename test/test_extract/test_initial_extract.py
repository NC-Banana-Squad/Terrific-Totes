from src.extract.extract import initial_extract
from src.extract.util_functions import (
    store_in_s3,
    format_to_csv,
    create_s3_client,
    connect,
    create_file_name,
)
import pytest
from unittest.mock import patch, MagicMock


class TestInitialExtract:

    @patch("src.extract.extract.store_in_s3")
    @patch("src.extract.extract.format_to_csv")
    @patch("src.extract.extract.create_file_name")
    @patch("src.extract.extract.connect")
    @patch("src.extract.extract.create_s3_client")
    def test_returns_success_for_successful_run(
        self,
        mock_create_s3_client,
        mock_connect,
        mock_create_file_name,
        mock_format_to_csv,
        mock_store_in_s3,
    ):
        mock_create_s3_client.return_value = MagicMock()
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.run.side_effect = [
            [{"table_name": "peaches"}],
            [{"cost_of_peaches": 132}],
        ]
        mock_conn.columns = [{"name": "cost_of_peaches"}]
        mock_create_file_name.return_value = "why_peaches.csv"
        mock_format_to_csv.return_value = MagicMock()
        mock_store_in_s3.return_value = None

        result = initial_extract(mock_create_s3_client, mock_conn)
        assert result == {
            "result": f"Object successfully created in banana-squad-code bucket"
        }

    @patch("src.extract.extract.connect")
    @patch("src.extract.extract.create_s3_client")
    def test_create_s3_client_failure(self, mock_create_s3_client, mock_connect):
        mock_create_s3_client.side_effect = Exception("S3 client creation error")

        with patch("src.extract.extract.logging.error") as mock_error:
            result = initial_extract(mock_create_s3_client, mock_connect)

        mock_error.assert_called_with(
            "Failed to create a client from create_client function: S3 client creation error"
        )
        assert result == {
            "result": "Failed to create an object in banana-squad-code bucket"
        }

        assert not mock_connect.called
