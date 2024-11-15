from src.extract.extract import initial_extract
from src.extract.util_functions import (
    store_in_s3,
    format_to_csv,
    create_s3_client,
    connect,
    create_file_name,
)
import pytest
from unittest.mock import patch, Mock


class TestInitialExtract:

    @patch("src.extract.util_functions.store_in_s3")
    @patch("src.extract.util_functions.format_to_csv")
    @patch("src.extract.util_functions.create_file_name")
    @patch("src.extract.util_functions.connect")
    @patch("src.extract.util_functions.create_s3_client")
    def xtest_returns_success_for_successful_run(
        self,
        mock_create_s3_client,
        mock_connect,
        mock_create_file_name,
        mock_format_to_csv,
        mock_store_in_s3
    ):
        mock_create_s3_client.return_value = Mock()
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.run.side_effect = [
            [{"table_name": "peaches"}],
            [{"cost_of_peaches": 132}],
        ]
        mock_conn.columns = [{"name": "cost_of_peaches"}]
        mock_create_file_name.return_value = "why_peaches.csv"
        mock_format_to_csv.return_value = Mock()
        mock_store_in_s3.return_value = None

        result = initial_extract()
        assert result == {
            "result": f"Object successfully created in banana-squad-code bucket"
        }

    @patch("src.extract.util_functions.create_s3_client")
    def xtest_create_s3_client_failure(self, mock_create_s3_client):
        mock_create_s3_client.side_effect = Exception("S3 client creation error")
        with patch("src.extract.extract.logging.error") as mock_error:
            result = initial_extract()
        mock_error.assert_called_with(
            "Failed to create a client from create_client function: S3 client creation error"
        )
        assert result == {
            "result": f"Failed to create an object in banana-squad-code bucket"
        }
