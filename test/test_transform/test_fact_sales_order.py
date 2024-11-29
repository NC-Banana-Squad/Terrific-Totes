import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from transform_utils import fact_sales_order


class TestFactSalesOrder(unittest.TestCase):
    @patch("transform_utils.connect")
    @patch("transform_utils.table_has_data")
    @patch("transform_utils.get_current_max_id")
    def test_no_existing_data(self, mock_get_current_max_id, mock_table_has_data, mock_connect):
        mock_table_has_data.return_value = False
        mock_connect.return_value = MagicMock()
        
        input_data = {
            "staff_id": [101, 102],
            "created_at": ["2024-11-27 12:30:00", "2024-11-28 15:45:00"],
            "last_updated": ["2024-11-27 13:00:00", "2024-11-28 16:15:00"],
        }
        df = pd.DataFrame(input_data)
        
        expected_sales_record_ids = [1, 2]
        
        result_df = fact_sales_order(df)
        
        self.assertListEqual(list(result_df["sales_record_id"]), expected_sales_record_ids)
        self.assertListEqual(list(result_df["sales_staff_id"]), input_data["staff_id"])
        self.assertListEqual(list(result_df["created_date"]), [pd.Timestamp("2024-11-27").date(), pd.Timestamp("2024-11-28").date()])
        self.assertIn("created_time", result_df.columns)
        self.assertIn("last_updated_date", result_df.columns)
        self.assertNotIn("created_at", result_df.columns)
    
    @patch("transform_utils.connect")
    @patch("transform_utils.table_has_data")
    @patch("transform_utils.get_current_max_id")
    def test_existing_data(self, mock_get_current_max_id, mock_table_has_data, mock_connect):
        mock_table_has_data.return_value = True
        mock_get_current_max_id.return_value = 5
        mock_connect.return_value = MagicMock()
        
        input_data = {
            "staff_id": [101, 102],
            "created_at": ["2024-11-27 12:30:00.123456", "2024-11-28 15:45:00"],
            "last_updated": ["2024-11-27 13:00:00.123456", "2024-11-28 16:15:00"],
        }
        df = pd.DataFrame(input_data)
        
        expected_sales_record_ids = [6, 7]
        
        result_df = fact_sales_order(df)
        
        self.assertListEqual(list(result_df["sales_record_id"]), expected_sales_record_ids)
        self.assertListEqual(list(result_df["sales_staff_id"]), input_data["staff_id"])
        self.assertListEqual(list(result_df["created_date"]), [pd.Timestamp("2024-11-27").date(), pd.Timestamp("2024-11-28").date()])
        self.assertIn("last_updated_time", result_df.columns)
        self.assertNotIn("last_updated", result_df.columns)    
   
    @patch("transform_utils.connect")
    @patch("transform_utils.table_has_data")
    @patch("transform_utils.get_current_max_id")
    def test_invalid_timestamps(self, mock_get_current_max_id, mock_table_has_data, mock_connect):
        mock_table_has_data.return_value = False
        mock_connect.return_value = MagicMock()
        
        input_data = {
            "staff_id": [101],
            "created_at": ["InvalidTimestamp"],
            "last_updated": ["AnotherInvalidTimestamp"],
        }
        df = pd.DataFrame(input_data)
        
        result_df = fact_sales_order(df)
        
        self.assertTrue(pd.isna(result_df["created_date"].iloc[0]))
        self.assertTrue(pd.isna(result_df["last_updated_date"].iloc[0]))
    
if __name__ == "__main__":
    unittest.main()
