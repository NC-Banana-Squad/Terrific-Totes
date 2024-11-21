import pandas as pd


def address(df):
    """Takes the only the address dataframe from the transform.py file read from s3 trigger.
    Should return transformed dataframe to be used by Lambda Handler.
    """

    # Rename columns
    rename_columns = {
        "address_id": "location_id",
        "address_line_1": "address_line_1",
        "address_line_2": "address_line_2",
        "district": "district",
        "city": "city",
        "postal_code": "postal_code",
        "country": "country",
        "phone": "phone",
    }
    df = df.rename(columns=rename_columns)

    # Replace missing data with null (NaN)- panadas'standard for "null"
    df = df.fillna(value=pd.NA)

    # Remove created_at and last_updated columns
    df = df.drop(columns=['created_at', 'last_updated'])
    
    return df

