import pandas as pd
from transform_utils import staff


def test_staff_basic_merge():
    # Input DataFrames
    staff_df = pd.DataFrame(
        {
            "staff_id": [1, 2],
            "first_name": ["John", "Jane"],
            "last_name": ["Doe", "Smith"],
            "department_id": [10, 20],
            "email_address": ["john.doe@example.com", "jane.smith@example.com"],
        }
    )
    department_df = pd.DataFrame(
        {
            "department_id": [10, 20],
            "department_name": ["HR", "Finance"],
            "location": ["New York", "London"],
        }
    )

    # Expected Output
    expected_output = pd.DataFrame(
        {
            "staff_id": [1, 2],
            "first_name": ["John", "Jane"],
            "last_name": ["Doe", "Smith"],
            "department_name": ["HR", "Finance"],
            "location": ["New York", "London"],
            "email_address": ["john.doe@example.com", "jane.smith@example.com"],
        }
    )

    # Run Function
    result = staff(staff_df, department_df)

    # Assert
    pd.testing.assert_frame_equal(result, expected_output)


def test_staff_with_missing_department():
    # Input DataFrames
    staff_df = pd.DataFrame(
        {
            "staff_id": [1, 2],
            "first_name": ["John", "Jane"],
            "last_name": ["Doe", "Smith"],
            "department_id": [
                10,
                30,
            ],  # Note: department_id 30 does not exist in department_df
            "email_address": ["john.doe@example.com", "jane.smith@example.com"],
        }
    )
    department_df = pd.DataFrame(
        {
            "department_id": [10, 20],
            "department_name": ["HR", "Finance"],
            "location": ["New York", "London"],
        }
    )

    # Expected Output
    expected_output = pd.DataFrame(
        {
            "staff_id": [1, 2],
            "first_name": ["John", "Jane"],
            "last_name": ["Doe", "Smith"],
            "department_name": ["HR", None],
            "location": ["New York", None],
            "email_address": ["john.doe@example.com", "jane.smith@example.com"],
        }
    )

    # Run Function
    result = staff(staff_df, department_df)

    result = result.where(pd.notnull(result), None)
    # Assert
    pd.testing.assert_frame_equal(result, expected_output)


def test_staff_removes_duplicates():
    # Input DataFrames with duplicate rows
    staff_df = pd.DataFrame(
        {
            "staff_id": [1, 1],
            "first_name": ["John", "John"],
            "last_name": ["Doe", "Doe"],
            "department_id": [10, 10],
            "email_address": ["john.doe@example.com", "john.doe@example.com"],
        }
    )
    department_df = pd.DataFrame(
        {"department_id": [10], "department_name": ["HR"], "location": ["New York"]}
    )

    # Expected Output
    expected_output = pd.DataFrame(
        {
            "staff_id": [1],
            "first_name": ["John"],
            "last_name": ["Doe"],
            "department_name": ["HR"],
            "location": ["New York"],
            "email_address": ["john.doe@example.com"],
        }
    )

    # Run Function
    result = staff(staff_df, department_df)

    # Assert
    pd.testing.assert_frame_equal(result, expected_output)


def test_staff_empty_inputs():
    # Input DataFrames
    staff_df = pd.DataFrame(
        columns=[
            "staff_id",
            "first_name",
            "last_name",
            "department_id",
            "email_address",
        ]
    )
    department_df = pd.DataFrame(
        columns=["department_id", "department_name", "location"]
    )

    # Expected Output
    expected_output = pd.DataFrame(
        columns=[
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ]
    )

    # Run Function
    result = staff(staff_df, department_df)

    # Assert
    pd.testing.assert_frame_equal(result, expected_output)
