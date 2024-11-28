import unittest
from unittest.mock import MagicMock, patch

class TestGetCurrentMaxId(unittest.TestCase):
    def setUp(self):
        self.mock_conn = MagicMock()

    def test_returns_max_id_when_not_none(self):
        self.mock_conn.run.return_value = [(42,)]
        result = get_current_max_id(self.mock_conn)
        self.assertEqual(result, 42)
        self.mock_conn.run.assert_called_once_with("SELECT MAX(sales_record_id) FROM fact_sales_order")

    def test_returns_zero_when_max_id_is_none(self):
        self.mock_conn.run.return_value = [(None,)]
        result = get_current_max_id(self.mock_conn)
        self.assertEqual(result, 0)
        self.mock_conn.run.assert_called_once_with("SELECT MAX(sales_record_id) FROM fact_sales_order")

    @patch("builtins.print")
    @patch("transform.logger.info")
    def test_logs_and_prints_result(self, mock_logger_info, mock_print):
        self.mock_conn.run.return_value = [(99,)]
        get_current_max_id(self.mock_conn)
        mock_print.assert_called_once_with([(99,)])
        mock_logger_info.assert_called_once_with([(99,)])

if __name__ == "__main__":
    unittest.main()
