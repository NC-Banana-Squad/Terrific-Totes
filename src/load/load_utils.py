import boto3
from io import BytesIO
from pg8000.native import Connection
import json
import pandas as pd
import logging
import os

# Constants
PROCESSED_BUCKET = "banana-squad-processed-data"

def get_secret(secret_name, region_name=None):
    """
    Retrieves a secret from AWS Secrets Manager

    Args:
        secret_name (str): The name of the secret in Secrets Manager
    Returns:
        dict: A dictionary of the secret values.
    """
    region_name = "eu-west-2"

    client = boto3.client("secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
            return json.loads(secret)
        else:
            raise ValueError("Secret is stored as binary; function expects JSON.")
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve secret: {e}")

def connect():
    """Gets a Connection to the ToteSys database.
    Credentials are retrieved from AWS Secrets Manager by invoking get_secrets().
    Returns:
        a connection to data warehouse
    """
    secret_name = "database_credentials"
    secret = get_secret(secret_name)

    user = secret["load_user"]
    database = secret["load_database"]
    password = secret["load_password"]
    host = secret["load_host"]
    port = secret["load_port"]

    return Connection(
        user=user, database=database, password=password, host=host, port=load_port
    )

def create_s3_client():
    """
    Creates an S3 client using boto3
    """

    return boto3.client("s3")

def load_parquet(s3_client, bucket_name, key, table_name, conn):
    """Loads a Parquet file from S3 into target table in the data warehouse."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        parquet_data = BytesIO(response["Body"].read())
        dataframe = pd.read_parquet(parquet_data)

        # Check if the DataFrame is empty
        if dataframe.empty:
            logging.warning(f"No data found in the Parquet file: {key}")
            return

        # Create table if it doesn't exist
        create_table(dataframe, table_name, conn)

        # Insert data into the table
        columns = ",".join([f'"{col}"' for col in dataframe.columns])
        placeholders = ",".join(["%s"] * len(dataframe.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Use run_many for batch inserts
        data = [tuple(row) for _, row in dataframe.iterrows()]
        conn.run_many(insert_query, data)

        logging.info(f"Data loaded successfully into {table_name}.")
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise

def create_table(dataframe, table_name, conn):
    """Creates a table in the data warehouse if it doesn't exist."""
    exists_query = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = '{table_name}'
    );
    """
    result = conn.run(exists_query)
    exists = result[0][0]  # Extract the boolean result

    if exists:
        logging.info(f"Table {table_name} already exists. Skipping creation.")
        return

    # Dynamically create the table schema based on the DataFrame
    column_definitions = []
    for column, dtype in dataframe.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            sql_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            sql_type = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql_type = "TIMESTAMP"
        else:
            sql_type = "TEXT"
        column_definitions.append(f'"{column}" {sql_type}')

    # Construct and execute the CREATE TABLE statement
    create_table_query = f"""
    CREATE TABLE {table_name} (
        {", ".join(column_definitions)}
    );
    """
    conn.run_ddl(create_table_query)

    logging.info(f"Table {table_name} created successfully.")