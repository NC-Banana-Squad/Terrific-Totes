import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import io
from load import load_parquet_from_s3

class TestLoadParquetFromS3(unittest.TestCase):
    @patch("pandas.read_parquet")
    @patch("boto3.client")  # Patching boto3.client to mock the S3 client
    @patch("load.logger")
    def test_load_parquet_success(self, mock_logger, mock_boto_client, mock_read_parquet):
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.get_object.return_value = {
            "Body": io.BytesIO(b"dummy parquet data")
        }
        mock_read_parquet.return_value = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        result = load_parquet_from_s3(mock_s3_client, "test-bucket", "test-key")

        mock_s3_client.get_object.assert_called_once_with(Bucket="test-bucket", Key="test-key")
        mock_read_parquet.assert_called_once()
        self.assertTrue(isinstance(result, pd.DataFrame))

    @patch("boto3.client")  # Patching boto3.client to mock the S3 client
    @patch("load.logger")
    def test_load_parquet_no_such_key(self, mock_logger, mock_boto_client):
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.get_object.side_effect = mock_s3_client.exceptions.NoSuchKey

        with self.assertRaises(mock_s3_client.exceptions.NoSuchKey):
            load_parquet_from_s3(mock_s3_client, "test-bucket", "test-key")

        mock_logger.error.assert_called_once_with("Key test-key not found in bucket test-bucket.")

    @patch("boto3.client")  # Patching boto3.client to mock the S3 client
    @patch("load.logger")
    def test_load_parquet_general_exception(self, mock_logger, mock_boto_client):
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.get_object.side_effect = Exception("Some error")

        with self.assertRaises(Exception):
            load_parquet_from_s3(mock_s3_client, "test-bucket", "test-key")

        mock_logger.error.assert_called_once_with("Error loading parquet file from S3: Some error")

if __name__ == "__main__":
    unittest.main()
