import pandas as pd


def design(df):
    """
    Transforms the design table into the dim_design table for the star schema.

    Args:
        df (pd.DataFrame): Raw design table DataFrame.

    Returns:
        dim_design (pd.DataFrame): Transformed dim_design DataFrame.
    """
    # Select relevant columns
    dim_design = df[["design_id", "design_name", "file_location", "file_name"]]

    # Drop duplicates
    dim_design = dim_design.drop_duplicates(subset=["design_id"])

    # Sorting is optional but recommended for consistent
    dim_design = dim_design.sort_values(by="design_id").reset_index(drop=True)

    return dim_design
