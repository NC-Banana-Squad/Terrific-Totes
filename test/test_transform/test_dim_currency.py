import pytest
import pandas as pd
from transform_utils import dim_currency


def test_currency_names():
    data = {
        "currency_id": [1, 2, 3],
        "currency_code": ["GBP", "USD", "EUR"],
        "created_at": [
            "2024-11-21 10:00:00",
            "2024-11-21 11:00:00",
            "2024-11-21 12:00:00",
        ],
        "last_updated": [
            "2024-11-21 10:30:00",
            "2024-11-21 11:30:00",
            "2024-11-21 12:30:00",
        ],
    }

    df = pd.DataFrame(data)

    result = dim_currency(df)

    expected_data = {
        "currency_id": [1, 2, 3],
        "currency_code": ["GBP", "USD", "EUR"],
        "currency_name": ["British Pound Sterling", "United States Dollar", "Euro"],
    }

    expected_df = pd.DataFrame(expected_data)

    assert result.equals(
        expected_df
    ), f"Test failed. Got: {result}, Expected: {expected_df}"


def test_invalid_currency_code():
    data = {
        "currency_id": [1, 2, 3],
        "currency_code": ["GBP", "JPY", "EUR"],
        "created_at": [
            "2024-11-21 10:00:00",
            "2024-11-21 11:00:00",
            "2024-11-21 12:00:00",
        ],
        "last_updated": [
            "2024-11-21 10:30:00",
            "2024-11-21 11:30:00",
            "2024-11-21 12:30:00",
        ],
    }
    df = pd.DataFrame(data)

    result = dim_currency(df)

    expected_data = {
        "currency_id": [1, 2, 3],
        "currency_code": ["GBP", "JPY", "EUR"],
        "currency_name": ["British Pound Sterling", None, "Euro"],
    }
    expected_df = pd.DataFrame(expected_data)

    assert result.equals(
        expected_df
    ), f"Test failed. Got: {result}, Expected: {expected_df}"

def test_missing_currency_code():
    data = {
        "currency_id": [1, 2, 3],
        "currency_code": ["GBP", None, "EUR"],
        "created_at": [
            "2024-11-21 10:00:00",
            "2024-11-21 11:00:00",
            "2024-11-21 12:00:00",
        ],
        "last_updated": [
            "2024-11-21 10:30:00",
            "2024-11-21 11:30:00",
            "2024-11-21 12:30:00",
        ],
    }
    df = pd.DataFrame(data)

    result = dim_currency(df)

    expected_data = {
        "currency_id": [1, 2, 3],
        "currency_code": ["GBP", None, "EUR"],
        "currency_name": ["British Pound Sterling", None, "Euro"],
    }
    expected_df = pd.DataFrame(expected_data)

    assert result.equals(
        expected_df
    ), f"Test failed. Got: {result}, Expected: {expected_df}"


def test_empty_dataframe():
    df = pd.DataFrame(
        columns=["currency_id", "currency_code", "created_at", "last_updated"]
    )

    result = dim_currency(df)

    expected_df = pd.DataFrame(
        columns=["currency_id", "currency_code", "currency_name"]
    )

    result.reset_index(drop=True, inplace=True)
    expected_df.reset_index(drop=True, inplace=True)

    assert result.equals(
        expected_df
    ), f"Test failed. Got: {result}, Expected: {expected_df}"


def test_missing_column():
    data = {
        "currency_id": [1, 2, 3],
        "created_at": [
            "2024-11-21 10:00:00",
            "2024-11-21 11:00:00",
            "2024-11-21 12:00:00",
        ],
        "last_updated": [
            "2024-11-21 10:30:00",
            "2024-11-21 11:30:00",
            "2024-11-21 12:30:00",
        ],
    }
    df = pd.DataFrame(data)

    with pytest.raises(KeyError):
        dim_currency(df)