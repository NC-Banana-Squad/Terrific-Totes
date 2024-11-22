import pytest
import pandas as pd
from transform_utils import date


def test():
    df_dim_date = dim_date()
    print(df_dim_date.head())


def test_dim_date_structure():
    # Test if the structure of the DataFrame is correct
    df = dim_date("2025-02-18", "2025-02-18")
    expected_columns = [
        "date_id",
        "year",
        "month",
        "day",
        "day_of_week",
        "day_name",
        "month_name",
        "quarter",
    ]
    assert list(df.columns) == expected_columns, "Column structure is incorrect."


def test_dim_date_range():
    # Test if the correct number of rows is returned
    start, end = "2025-02-18", "2025-02-18"
    df = dim_date(start, end)
    expected_rows = (pd.to_datetime(end) - pd.to_datetime(start)).days + 1
    assert len(df) == expected_rows, "Row count does not match the date range."
