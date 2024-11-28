import pandas as pd
from transform_utils import dim_location
import pytest


def test_return_expected_data():

    input_data = pd.DataFrame(
        {
            "address_id": [1],
            "address_line_1": ["123 Main St"],
            "address_line_2": ["Apt 1"],
            "district": ["Bedfordshire"],
            "city": ["Beaulahcester"],
            "postal_code": ["12345"],
            "country": ["USA"],
            "phone": ["555-1234"],
            "created_at": ["2023-01-01"],
            "last_updated": ["2023-01-05"],
        }
    )

    expected_data = pd.DataFrame(
        {
            "location_id": [1],
            "address_line_1": ["123 Main St"],
            "address_line_2": ["Apt 1"],
            "district": ["Bedfordshire"],
            "city": ["Beaulahcester"],
            "postal_code": ["12345"],
            "country": ["USA"],
            "phone": ["555-1234"],
        }
    )

    result = dim_location(input_data)

    assert list(result.columns) == list(expected_data.columns)
    assert (result.values == expected_data.values).all()
    assert result.shape == expected_data.shape
    assert result.equals(expected_data)


def test_return_expected_data_with_missing_values():

    input_data = pd.DataFrame(
        {
            "address_id": [1],
            "address_line_1": ["123 Main St"],
            "address_line_2": ["Apt 1"],
            "district": [None],
            "city": ["Beaulahcester"],
            "postal_code": ["12345"],
            "country": ["USA"],
            "phone": ["555-1234"],
            "created_at": ["2023-01-01"],
            "last_updated": ["2023-01-05"],
        }
    )

    expected_data = pd.DataFrame(
        {
            "location_id": [1],
            "address_line_1": ["123 Main St"],
            "address_line_2": ["Apt 1"],
            "district": [pd.NA],
            "city": ["Beaulahcester"],
            "postal_code": ["12345"],
            "country": ["USA"],
            "phone": ["555-1234"],
        }
    )

    result = dim_location(input_data)

    assert result.equals(expected_data)
