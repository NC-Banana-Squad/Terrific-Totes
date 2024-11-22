import pandas as pd
from transform_utils import address
import pytest

def test_return_expected_data():
    #Mock input DataFrame
    #Arrange
    input_data = pd.DataFrame({
        "address_id": [1],
        "address_line_1": ["123 Main St"],
        "address_line_2": ["Apt 1"],
        "district": ["Bedfordshire"],
        "city": ["Beaulahcester"],
        "postal_code": ["12345"],
        "country": ["USA"],
        "phone": ["555-1234"],
        "created_at": ["2023-01-01"],
        "last_updated": ["2023-01-05"]
        })

    expected_data = pd.DataFrame({
        "location_id": [1],
        "address_line_1": ["123 Main St"],
        "address_line_2": ["Apt 1"],
        "district": ["Bedfordshire"],
        "city": ["Beaulahcester"],
        "postal_code": ["12345"],
        "country": ["USA"],
        "phone": ["555-1234"]
        })


    #Action
    result = address(input_data)

    #Assert
    #Ensures the column names are the same
    assert list(result.columns) == list(expected_data.columns)
    #Ensure dataframe values are equal
    assert (result.values == expected_data.values).all()
    #Ensure the number of rows and columns are as expected
    assert result.shape == expected_data.shape
    # Ensure result matches expected_data
    assert result.equals(expected_data)

    


def test_return_expected_data_with_missing_values():

    #Arrange
    input_data = pd.DataFrame({
        "address_id": [1],
        "address_line_1": ["123 Main St"],
        "address_line_2": ["Apt 1"],
        "district": [None],
        "city": ["Beaulahcester"],
        "postal_code": ["12345"],
        "country": ["USA"],
        "phone": ["555-1234"],
        "created_at": ["2023-01-01"],
        "last_updated": ["2023-01-05"]
        })

    expected_data = pd.DataFrame({
        "location_id": [1],
        "address_line_1": ["123 Main St"],
        "address_line_2": ["Apt 1"],
        "district": [pd.NA],
        "city": ["Beaulahcester"],
        "postal_code": ["12345"],
        "country": ["USA"],
        "phone": ["555-1234"]
        })


    #Action
    result = address(input_data)

    #Assert
    # Ensure result matches expected_data
    assert result.equals(expected_data)
  







