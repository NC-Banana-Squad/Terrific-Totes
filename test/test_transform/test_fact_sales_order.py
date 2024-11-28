# import unittest
# import pandas as pd
# from transform import fact_sales_order

# class TestFactSalesOrder(unittest.TestCase):
#     def setUp(self):
#         self.input_data = pd.DataFrame({
#             "created_at": ["2024-11-28 10:15:30", "2024-11-28 10:15:30.123456"],
#             "last_updated": ["2024-11-28 12:00:00", "2024-11-28 12:00:00.654321"]
#         })
#         self.expected_output = pd.DataFrame({
#             "created_date": [pd.Timestamp("2024-11-28").date()] * 2,
#             "created_time": [pd.Timestamp("2024-11-28 10:15:30").time(),
#                              pd.Timestamp("2024-11-28 10:15:30.123456").time()],
#             "last_updated_date": [pd.Timestamp("2024-11-28").date()] * 2,
#             "last_updated_time": [pd.Timestamp("2024-11-28 12:00:00").time(),
#                                   pd.Timestamp("2024-11-28 12:00:00.654321").time()]
#         })

#     def test_fact_sales_order_transformation(self):
#         result = fact_sales_order(self.input_data.copy())
#         pd.testing.assert_frame_equal(result, self.expected_output)

#     def test_handles_missing_milliseconds(self):
#         input_data = pd.DataFrame({
#             "created_at": ["2024-11-28 14:30:45"],
#             "last_updated": ["2024-11-28 14:31:15"]
#         })
#         expected_output = pd.DataFrame({
#             "created_date": [pd.Timestamp("2024-11-28").date()],
#             "created_time": [pd.Timestamp("2024-11-28 14:30:45").time()],
#             "last_updated_date": [pd.Timestamp("2024-11-28").date()],
#             "last_updated_time": [pd.Timestamp("2024-11-28 14:31:15").time()]
#         })
#         result = fact_sales_order(input_data)
#         pd.testing.assert_frame_equal(result, expected_output)

#     def test_handles_invalid_dates(self):
#         input_data = pd.DataFrame({
#             "created_at": ["invalid_date"],
#             "last_updated": ["invalid_date"]
#         })
#         expected_output = pd.DataFrame({
#             "created_date": [pd.NaT],
#             "created_time": [pd.NaT],
#             "last_updated_date": [pd.NaT],
#             "last_updated_time": [pd.NaT]
#         })
#         result = fact_sales_order(input_data)
#         pd.testing.assert_frame_equal(result, expected_output)

# if __name__ == "__main__":
#     unittest.main()

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from transform_utils import fact_sales_order  # Replace 'your_module' with the actual module name


class TestFactSalesOrder(unittest.TestCase):
    @patch("transform_utils.connect")
    @patch("transform_utils.table_has_data")
    @patch("transform_utils.get_current_max_id")
    def test_no_existing_data(self, mock_get_current_max_id, mock_table_has_data, mock_connect):
        # Arrange
        mock_table_has_data.return_value = False
        mock_connect.return_value = MagicMock()
        
        input_data = {
            "staff_id": [101, 102],
            "created_at": ["2024-11-27 12:30:00", "2024-11-28 15:45:00"],
            "last_updated": ["2024-11-27 13:00:00", "2024-11-28 16:15:00"],
        }
        df = pd.DataFrame(input_data)
        
        expected_sales_record_ids = [1, 2]
        
        # Act
        result_df = fact_sales_order(df)
        
        # Assert
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
        # Arrange
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
        
        # Act
        result_df = fact_sales_order(df)
        
        # Assert
        self.assertListEqual(list(result_df["sales_record_id"]), expected_sales_record_ids)
        self.assertListEqual(list(result_df["sales_staff_id"]), input_data["staff_id"])
        self.assertListEqual(list(result_df["created_date"]), [pd.Timestamp("2024-11-27").date(), pd.Timestamp("2024-11-28").date()])
        self.assertIn("last_updated_time", result_df.columns)
        self.assertNotIn("last_updated", result_df.columns)    
   
    @patch("transform_utils.connect")
    @patch("transform_utils.table_has_data")
    @patch("transform_utils.get_current_max_id")
    def test_invalid_timestamps(self, mock_get_current_max_id, mock_table_has_data, mock_connect):
        # Arrange
        mock_table_has_data.return_value = False
        mock_connect.return_value = MagicMock()
        
        input_data = {
            "staff_id": [101],
            "created_at": ["InvalidTimestamp"],
            "last_updated": ["AnotherInvalidTimestamp"],
        }
        df = pd.DataFrame(input_data)
        
        # Act
        result_df = fact_sales_order(df)
        
        # Assert
        self.assertTrue(pd.isna(result_df["created_date"].iloc[0]))
        self.assertTrue(pd.isna(result_df["last_updated_date"].iloc[0]))
    
if __name__ == "__main__":
    unittest.main()
