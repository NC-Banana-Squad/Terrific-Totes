import unittest
from unittest.mock import MagicMock, patch
from transform_utils import table_has_data

class TestTableHasData(unittest.TestCase):
    def setUp(self):
        self.mock_conn = MagicMock()

    def test_returns_true_when_data_exists(self):
        self.mock_conn.run.return_value = [(True,)]
        result = table_has_data(self.mock_conn)
        self.assertTrue(result)
        self.mock_conn.run.assert_called_once_with("SELECT EXISTS (SELECT 1 FROM fact_sales_order LIMIT 1)")

    def test_returns_false_when_no_data(self):
        self.mock_conn.run.return_value = [(False,)]
        result = table_has_data(self.mock_conn)
        self.assertFalse(result)
        self.mock_conn.run.assert_called_once_with("SELECT EXISTS (SELECT 1 FROM fact_sales_order LIMIT 1)")

    @patch("builtins.print")
    @patch("transform.logger.info")
    def test_logs_and_prints_result(self, mock_logger_info, mock_print):
        self.mock_conn.run.return_value = [(True,)]
        table_has_data(self.mock_conn)
        mock_print.assert_called_once_with([(True,)])
        mock_logger_info.assert_called_once_with([(True,)])

if __name__ == "__main__":
    unittest.main()
