import pandas as pd


def sales_order(df):
    """Takes the dataframe from the transform.py file read from s3 trigger.
    Should return transformed dataframe to be used by Lambda Handler.
    """
    # created_at and last_updated columns have both the date and time in the column.
    # These both need to be split out for the fact table
    # this is needed for the sales_order split. But working on error fix
    df["created_at"] = df["created_at"].apply(
        lambda x: x if "." in x else x + ".000000"
    )
    df["created_at"] = pd.to_datetime(
        df["created_at"], format="%Y-%m-%d %H:%M:%S.%f", errors="coerce"
    )
    df["created_date"] = df["created_at"].dt.date
    df["created_time"] = df["created_at"].dt.time

    df["last_updated"] = df["last_updated"].apply(
        lambda x: x if "." in x else x + ".000000"
    )
    df["last_updated"] = pd.to_datetime(
        df["last_updated"], format="%Y-%m-%d %H:%M:%S.%f", errors="coerce"
    )
    df["last_updated_date"] = df["last_updated"].dt.date
    df["last_updated_time"] = df["last_updated"].dt.time

    df.drop(columns=["created_at", "last_updated"], inplace=True)
    return df