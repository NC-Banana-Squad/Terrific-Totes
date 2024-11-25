import pandas as pd


def counterparty(df1, df2):
    """
    Takes two dataframes from the transform.py file read from s3 trigger:
    1. counterparty
    2. address

    Should return transformed dataframe to be used by
    Lambda Handler for dim_counterparty

    Takes counterparty dataframe (df1) with:
        # counterparty_id eg 1
        # counterparty_legal_name eg Fahey & Sons
        # legal_address_id eg 28
        # commercial_contact eg Jean Hane II
        # created_at
        # last_updated

    Takes address dataframe (df2) with:

        # address_id eg 1
        # address_line_1 eg 179 Alexie Cliffs
        # address_line_2 eg None (not populated ...)
        # district eg Avon (most not populated ...)
        # city eg Lake Charles
        # postal_code eg 28441
        # country eg Turkey
        # phone eg 1803 637401
        # created_at
        # last_updated

    Returns dim_counterparty dataframe with:
        # counterparty_id
        # counterparty_legal_name
        # counterparty_legal_address_line_1
        # counterparty_legal_address_line_2
        # counterparty_legal_district
        # counteryparty_legal_city
        # counterparty_legal_postal_code
        # counterparty_legal_country
        # counterparty_legal_phone_number
    """

    # Perform an inner join on legal_address_id and address_id
    merged_df = pd.merge(
        df1, df2, left_on="legal_address_id", right_on="address_id", how="inner"
    )

    # Select and rename the relevant columns
    dim_counterparty = merged_df[
        [
            "counterparty_id",
            "counterparty_legal_name",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]
    ].rename(
        columns={
            "address_line_1": "counterparty_legal_address_line_1",
            "address_line_2": "counterparty_legal_address_line_2",
            "district": "counterparty_legal_district",
            "city": "counterparty_legal_city",
            "postal_code": "counterparty_legal_postal_code",
            "country": "counterparty_legal_country",
            "phone": "counterparty_legal_phone_number",
        }
    )

    return dim_counterparty
