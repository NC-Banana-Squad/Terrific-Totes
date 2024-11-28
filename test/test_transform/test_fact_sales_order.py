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
import pandas as pd
from moto import mock_aws
import boto3
import os
from transform import fact_sales_order

class TestFactSalesOrder(unittest.TestCase):
    @mock_aws
    def setUp(self):
        # Mock AWS Secrets Manager
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"  # Set a default region
        client = boto3.client("secretsmanager")
        client.create_secret(Name="my_test_secret", SecretString="mocked_value")
        
        # Prepare test data
        self.input_data = pd.DataFrame({
            "created_at": ["2024-11-28 10:15:30", "2024-11-28 10:15:30.123456"],
            "last_updated": ["2024-11-28 12:00:00", "2024-11-28 12:00:00.654321"]
        })
        self.expected_output = pd.DataFrame({
            "created_date": [pd.Timestamp("2024-11-28").date()] * 2,
            "created_time": [pd.Timestamp("2024-11-28 10:15:30").time(),
                             pd.Timestamp("2024-11-28 10:15:30.123456").time()],
            "last_updated_date": [pd.Timestamp("2024-11-28").date()] * 2,
            "last_updated_time": [pd.Timestamp("2024-11-28 12:00:00").time(),
                                  pd.Timestamp("2024-11-28 12:00:00.654321").time()]
        })

    def test_fact_sales_order_transformation(self):
        result = fact_sales_order(self.input_data.copy())
        pd.testing.assert_frame_equal(result, self.expected_output)

    def test_handles_missing_milliseconds(self):
        input_data = pd.DataFrame({
            "created_at": ["2024-11-28 14:30:45"],
            "last_updated": ["2024-11-28 14:31:15"]
        })
        expected_output = pd.DataFrame({
            "created_date": [pd.Timestamp("2024-11-28").date()],
            "created_time": [pd.Timestamp("2024-11-28 14:30:45").time()],
            "last_updated_date": [pd.Timestamp("2024-11-28").date()],
            "last_updated_time": [pd.Timestamp("2024-11-28 14:31:15").time()]
        })
        result = fact_sales_order(input_data)
        pd.testing.assert_frame_equal(result, expected_output)

    def test_handles_invalid_dates(self):
        input_data = pd.DataFrame({
            "created_at": ["invalid_date"],
            "last_updated": ["invalid_date"]
        })
        expected_output = pd.DataFrame({
            "created_date": [pd.NaT],
            "created_time": [pd.NaT],
            "last_updated_date": [pd.NaT],
            "last_updated_time": [pd.NaT]
        })
        result = fact_sales_order(input_data)
        pd.testing.assert_frame_equal(result, expected_output)


if __name__ == "__main__":
    unittest.main()