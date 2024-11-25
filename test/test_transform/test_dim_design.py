import pytest
import pandas as pd
from transform_utils import dim_design


# Test cases
def test_design_basic_transformation():
    # Mock input DataFrame
    input_data = {
        "design_id": [1, 2, 1],
        "design_name": ["Design A", "Design B", "Design A"],
        "file_location": ["/path/a", "/path/b", "/path/a"],
        "file_name": ["a.pdf", "b.pdf", "a.pdf"],
        "created_at": ["2024-01-01", "2024-01-02", "2024-01-01"],
        "last_updated": ["2024-02-01", "2024-02-02", "2024-02-01"],
    }
    df = pd.DataFrame(input_data)

    # Expected output DataFrame
    expected_data = {
        "design_id": [1, 2],
        "design_name": ["Design A", "Design B"],
        "file_location": ["/path/a", "/path/b"],
        "file_name": ["a.pdf", "b.pdf"],
    }
    expected_df = pd.DataFrame(expected_data)

    # Run the function
    result_df = dim_design(df)

    # Assert DataFrame equality
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_design_no_duplicates():
    # Mock input DataFrame (no duplicates)
    input_data = {
        "design_id": [1, 2],
        "design_name": ["Design A", "Design B"],
        "file_location": ["/path/a", "/path/b"],
        "file_name": ["a.pdf", "b.pdf"],
    }
    df = pd.DataFrame(input_data)

    # Expected output DataFrame (same as input)
    expected_df = pd.DataFrame(input_data)

    # Run the function
    result_df = dim_design(df)

    # Assert DataFrame equality
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_design_empty_input():
    # Mock empty DataFrame
    df = pd.DataFrame(
        columns=["design_id", "design_name", "file_location", "file_name"]
    )

    # Expected output is also an empty DataFrame
    expected_df = pd.DataFrame(
        columns=["design_id", "design_name", "file_location", "file_name"]
    )

    # Run the function
    result_df = dim_design(df)

    # Assert DataFrame equality
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_design_missing_columns():
    # Mock input DataFrame with missing column
    input_data = {"design_id": [1, 2], "design_name": ["Design A", "Design B"]}
    df = pd.DataFrame(input_data)

    # Ensure the function raises an error for missing columns
    with pytest.raises(KeyError):
        dim_design(df)


def test_design_extra_columns():
    # Mock input DataFrame with extra columns
    input_data = {
        "design_id": [1, 2],
        "design_name": ["Design A", "Design B"],
        "file_location": ["/path/a", "/path/b"],
        "file_name": ["a.pdf", "b.pdf"],
        "extra_column": [42, 43],
    }
    df = pd.DataFrame(input_data)

    # Expected output DataFrame (ignoring extra columns)
    expected_data = {
        "design_id": [1, 2],
        "design_name": ["Design A", "Design B"],
        "file_location": ["/path/a", "/path/b"],
        "file_name": ["a.pdf", "b.pdf"],
    }
    expected_df = pd.DataFrame(expected_data)

    # Run the function
    result_df = dim_design(df)

    # Assert DataFrame equality
    pd.testing.assert_frame_equal(result_df, expected_df)
