import unittest
from unittest.mock import patch
from datetime import datetime
from src.extract import create_file_name 
import pytest

class TestCreateFileName(unittest.TestCase):
    def test_returns_expected_file_name(self):
        fixed_datetime = datetime(2023, 12, 25, 15, 30, 45, 456457)
        table = "test_table"
        
        with patch('src.extract.datetime') as mock_datetime:
            mock_datetime.now.return_value = fixed_datetime
            
            file_name = create_file_name(table)
            
            expected_file_name = "test_table/2023/12/25/2023-12-25T15:30:45.456457.csv"
            
            assert file_name == expected_file_name


    def test_raises_exception_when_table_name_is_missing(self):
            table = ""
            with pytest.raises(ValueError):
                create_file_name(table)