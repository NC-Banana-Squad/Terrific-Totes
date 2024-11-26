import boto3
from io import BytesIO
from pg8000.native import Connection
import json
import pandas as pd
import logging
import os
from botocore.exceptions import ClientError

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
    secret_name = "datawarehouse_credentials"
    secret = get_secret(secret_name)

    user = secret["user"]
    database = secret["database"]
    password = secret["password"]
    host = secret["host"]
    port = secret["port"]

    return Connection(
        user=user, database=database, password=password, host=host, port=port
    )

def create_s3_client():
    """
    Creates an S3 client using boto3
    """

    return boto3.client("s3")


def load_parquet(s3_client, bucket_name, key, table_name, conn):
    """
    Loads a Parquet file from S3 into the target table in the data warehouse.

    Parameters:
        s3_client: An initialized boto3 S3 client.
        bucket_name (str): Name of the S3 bucket.
        key (str): The decoded key of the Parquet file in the S3 bucket.
        table_name: Fully qualified table name (including schema).
        conn: A pg8000.native.Connection object for executing SQL.
    """
    try:
        logging.info(f"Fetching file from bucket={bucket_name}, key={key}")

        # Fetch the Parquet file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        parquet_data = BytesIO(response["Body"].read())
        dataframe = pd.read_parquet(parquet_data)

        # Check if the DataFrame is empty
        if dataframe.empty:
            logging.warning(f"No data found in the Parquet file: {key}")
            return

        # Prepare the INSERT query with schema-qualified table names
        columns = ",".join([f'"{col}"' for col in dataframe.columns])
        placeholders = ",".join([f"${i+1}" for i in range(len(dataframe.columns))])
        schema_qualified_table_name = f'"{table_name.replace(".", "\".\"")}"'
        insert_query = f"INSERT INTO {schema_qualified_table_name} ({columns}) VALUES ({placeholders})"

        # Prepare data for batch insert
        data = [tuple(row) for _, row in dataframe.iterrows()]
        logging.info(f"Preparing to insert {len(data)} rows into {schema_qualified_table_name}")

        # Execute the batch insert row by row
        for row in data:
            conn.run(insert_query, row)  # Pass row as a single argument (tuple)

        logging.info(f"Data loaded successfully into {schema_qualified_table_name}.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logging.error(f"Key {key} not found in bucket {bucket_name}.")
            raise FileNotFoundError(f"The key '{key}' does not exist in bucket '{bucket_name}'.")
        else:
            raise
    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        raise