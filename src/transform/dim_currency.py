import pandas as pd

def currency(df):
    """Takes the dataframe from the transform.py file read from s3 trigger.
    Should return transformed dataframe to be used by Lambda Handler.

    # currency table has:
        # currency_id
        # currency_code (GBP, USD, EUR)
        # created_at
        # last_updated

    # dim_currency table has:
        # currency_id
        # currency_code
        # currency_name
    """

    currency_map = {
        "GBP": "British Pound Sterling",
        "USD": "United States Dollar",
        "EUR": "Euro",
    }

    # Maps 'currency_code' to 'currency_name' using currency_map
    df["currency_name"] = df["currency_code"].map(currency_map)

    df.drop(columns=["created_at", "last_updated"], inplace=True)

    return df