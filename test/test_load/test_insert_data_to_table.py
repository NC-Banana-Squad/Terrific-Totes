import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from load import insert_data_to_table

class TestInsertDataToTable(unittest.TestCase):
    @patch("load.logger")
    def test_insert_data_success(self, mock_logger):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        insert_data_to_table(mock_conn, "test_table", df)

        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()
        mock_logger.info.assert_any_call("Inserting data into table test_table with conflict handling...")
        mock_logger.info.assert_any_call("Data successfully inserted/updated into table test_table.")

    @patch("load.logger")
    def test_insert_data_exception(self, mock_logger):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Some error")
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        with self.assertRaises(Exception):
            insert_data_to_table(mock_conn, "test_table", df)

        mock_conn.rollback.assert_called_once()
        mock_logger.error.assert_called_once_with("Error inserting data into test_table: Some error")

if __name__ == "__main__":
    unittest.main()