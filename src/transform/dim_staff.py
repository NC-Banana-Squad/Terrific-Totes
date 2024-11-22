import pandas as pd

def staff(df1, df2):

    """
    Transforms the staff and department DataFrames to create dim_staff.
    
    Args:
        staff_df (pd.DataFrame): DataFrame containing staff table data.
        department_df (pd.DataFrame): DataFrame containing department table data.
    
    Returns:
        pd.DataFrame: Transformed DataFrame for the dim_staff table.
    """
    staff_df = df1
    department_df = df2

    # Merge staff with department to include department details
    dim_staff = staff_df.merge(
        department_df[["department_id", "department_name", "location"]],
        how="left",
        on="department_id"
    )
    
    # Select relevant columns
    dim_staff = dim_staff[[
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address"
    ]]

    # Remove duplicates if any
    dim_staff = dim_staff.drop_duplicates()

    # Sort for consistency and reset index
    dim_staff = dim_staff.sort_values(by="staff_id").reset_index(drop=True)

    return dim_staff
