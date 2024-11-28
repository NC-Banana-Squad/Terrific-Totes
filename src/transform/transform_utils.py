import boto3
import pg8000
import pandas as pd
import logging
import json
import io
import pg8000.native

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secret(secret_name, region_name="eu-west-2"):
    """
    Retrieves a secret from AWS Secrets Manager.

    Args:
        secret_name (str): The name of the secret in Secrets Manager
        region_name (str): The AWS region where the Secrets Manager is hosted
    Returns:
        dict: A dictionary of the secret values.
    """
    logger.info("Fetching database secrets...")
    try:
        client = boto3.client("secretsmanager", region_name=region_name)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
            return json.loads(secret)
        else:
            raise ValueError("Secret is stored as binary; function expects JSON.")
    except Exception as e:
        logger.error(f"Failed to retrieve secret: {e}")
        raise RuntimeError(f"Failed to retrieve secret: {e}")

def connect():
    """
    Establish a connection to the ToteSys database.
    Credentials are retrieved from AWS Secrets Manager by invoking get_secret().
    Returns:
        pg8000.Connection: A connection to the data warehouse
    """
    logger.info("Establishing database connection...")
    secret_name = "datawarehouse_credentials"
    secret = get_secret(secret_name)

    try:
        conn = pg8000.connect(
            user=secret["user"],
            database=secret["database"],
            password=secret["password"],
            host=secret["host"],
            port=secret["port"]
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise RuntimeError(f"Error connecting to database: {e}")    

def table_has_data(conn):
    query = "SELECT EXISTS (SELECT 1 FROM fact_sales_order LIMIT 1)"
    result = conn.run(query)
    print(result)
    logger.info(result)
    return result[0][0]

def get_current_max_id(conn):
    query = "SELECT MAX(sales_record_id) FROM fact_sales_order"
    result = conn.run(query)
    print(result)
    logger.info(result)
    return result[0][0] if result[0][0] is not None else 0

# def fact_sales_order(df):
#     """Takes the dataframe from the transform.py file read from s3 trigger.
#     Should return transformed dataframe to be used by Lambda Handler.
#     """

#     if 'staff_id' in df.columns:
#         df.rename(columns={'staff_id': 'sales_staff_id'}, inplace=True)

#     df["created_at"] = df["created_at"].apply(
#         lambda x: x if "." in x else x + ".000000"
#     )
#     df["created_at"] = pd.to_datetime(
#         df["created_at"], format="%Y-%m-%d %H:%M:%S.%f", errors="coerce"
#     )
#     df["created_date"] = df["created_at"].dt.date
#     df["created_time"] = df["created_at"].dt.time   
#     df["last_updated"] = df["last_updated"].apply(
#         lambda x: x if "." in x else x + ".000000"
#     )
#     df["last_updated"] = pd.to_datetime(
#         df["last_updated"], format="%Y-%m-%d %H:%M:%S.%f", errors="coerce"
#     )
#     df["last_updated_date"] = df["last_updated"].dt.date
#     df["last_updated_time"] = df["last_updated"].dt.time
#     df.drop(columns=["created_at", "last_updated"], inplace=True)
#     return df

def fact_sales_order(df):
    """Takes the dataframe from the transform.py file read from s3 trigger.
    Should return transformed dataframe to be used by Lambda Handler.
    """

    conn = connect()

    if table_has_data(conn):
        current_max_id = get_current_max_id(conn)
        df["sales_record_id"] = range(current_max_id + 1, current_max_id + len(df) + 1)
    
    else:
        df["sales_record_id"] = range(1, len(df) + 1)
    
    conn.close()

    df.fillna(value=pd.NA, inplace=True)

    df.rename(columns={"staff_id": "sales_staff_id"}, inplace=True)

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

def dim_counterparty(df1, df2):
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

    merged_df = pd.merge(
        df1, df2, left_on="legal_address_id", right_on="address_id", how="inner"
    )

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

def dim_currency(df):
    """Takes the dataframe from the transform.py file read from s3 trigger.
    Returns cleaned, normalised and transformed dataframe to be used by Lambda Handler.

    Takes dataframe with:
        # currency_id
        # currency_code (GBP, USD, EUR)
        # created_at
        # last_updated

    Returns dataframe with:
        # currency_id
        # currency_code
        # currency_name
    """

    currency_map = {
        "GBP": "British Pound Sterling",
        "USD": "United States Dollar",
        "EUR": "Euro"
    }

    df["currency_name"] = df["currency_code"].map(currency_map)

    df.drop(columns=["created_at", "last_updated"], inplace=True)

    return df


def dim_date(start="2022-01-01", end="2024-12-31"):
    """Creates calendar table for star schema.

    Arguments:
        # start date (YYYY-MM-DD)
        # end date (YYYY-MM-DD)

    Returns dataframe with:
        # date_id (INT - unique - YYYYMMDD format)
        # year eg 2024
        # month eg 01
        # day eg 24
        # day_of_week eg 02
        # day_name eg Tuesday
        # month_name eg February
        # quarter eg 04
    """

    dates = pd.date_range(start=start, end=end)
    df = pd.DataFrame(
        {
            "date_id": dates.strftime("%Y%m%d").astype(int),
            "year": dates.year,
            "month": dates.month,
            "day": dates.day,
            "day_of_week": dates.dayofweek,
            "day_name": dates.day_name(),
            "month_name": dates.month_name(),
            "quarter": dates.quarter,
        }
    )

    return df

def dim_design(df):
    """
    Transforms the design table into the dim_design table for the star schema.

    Args:
        df (pd.DataFrame): Raw design table DataFrame.

    Returns:
        dim_design (pd.DataFrame): Transformed dim_design DataFrame.
    """
    dim_design = df[["design_id", "design_name", "file_location", "file_name"]]

    dim_design = dim_design.drop_duplicates(subset=["design_id"])

    dim_design = dim_design.sort_values(by="design_id").reset_index(drop=True)

    return dim_design

def dim_location(df):
    """Takes the only the address dataframe from the transform.py file read from s3 trigger.
    Should return transformed dataframe to be used by Lambda Handler.
    """

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
    dim_location = df.rename(columns=rename_columns)

    dim_location = dim_location.fillna(value=pd.NA)

    dim_location = dim_location.drop(columns=["created_at", "last_updated"])

    return dim_location

def dim_staff(df1, df2):
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

    dim_staff = staff_df.merge(
        department_df[["department_id", "department_name", "location"]],
        how="left",
        on="department_id",
    )

    dim_staff = dim_staff[
        [
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ]
    ]

    dim_staff = dim_staff.drop_duplicates()

    dim_staff = dim_staff.sort_values(by="staff_id").reset_index(drop=True)

    return dim_staff