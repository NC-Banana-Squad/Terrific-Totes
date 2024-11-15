import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from io import StringIO
import pytest

from src.extract.extract import continuous_extract
from src.extract.util_functions import create_s3_client, connect

class TestContinuousExtract(unittest.TestCase):
    def setUp(self):
        # Mock data for testing
        self.mock_table_data = [('table1',)]
        self.mock_rows = [
            {'id': 1, 'name': 'Test', 'created_at': '2024-01-01 00:00:00'},
            {'id': 2, 'name': 'Test2', 'created_at': '2024-01-02 00:00:00'}
        ]
        self.mock_columns = [{'name': 'id'}, {'name': 'name'}, {'name': 'created_at'}]
        
    @patch('src.extract.util_functions.create_s3_client')
    @patch('src.extract.util_functions.connect')
    def xtest_continuous_extract_successful_extraction(self, mock_connect, mock_create_s3_client):
        # Set up mock S3 client
        mock_s3 = MagicMock()
        mock_create_s3_client.return_value = mock_s3
        
        # Mock S3 get_object response
        mock_s3.get_object.return_value = {
            'Body': MagicMock(
                read=lambda: '2024-01-01 00:00:00'.encode('utf-8')
            )
        }
        
        # Set up mock database connection
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Mock database query responses
        mock_conn.run.side_effect = [
            self.mock_table_data,  # Response for table names query
            self.mock_rows         # Response for data query
        ]
        mock_conn.columns = self.mock_columns
        
        # Execute the function
        result = continuous_extract()
        
        # Assertions
        self.assertEqual(result, {"result": "Success"})
        mock_s3.get_object.assert_called_once_with(
            Bucket='banana-squad-code',
            Key='last_extracted.txt'
        )
        
        # Verify database queries were made
        self.assertEqual(mock_conn.run.call_count, 2)
        mock_conn.close.assert_called_once()
        
        # Verify S3 store operation was called
        mock_s3.put_object.assert_called()  # Verify file was stored in S3


