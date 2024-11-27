import boto3
import pg8000
import pandas as pd
import logging
import json
import io

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
PROCESSED_BUCKET = "banana-squad-processed-data"

# Utility: Get Secret
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


# Utility: Create Database Connection
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


# Utility: Create S3 Client
def create_s3_client():
    """Initialize and return an S3 client."""
    logger.info("Creating S3 client...")
    return boto3.client("s3")


def load_parquet_from_s3(s3_client, bucket, key):
    """Download and read a Parquet file from S3 into a Pandas DataFrame."""
    logger.info(f"Loading Parquet file from S3: bucket={bucket}, key={key}")
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = io.BytesIO(obj["Body"].read())
        return pd.read_parquet(body)
    except s3_client.exceptions.NoSuchKey:
        logger.error(f"Key {key} not found in bucket {bucket}.")
        raise
    except Exception as e:
        logger.error(f"Error loading parquet file from S3: {e}")
        raise

# Utility: Insert Data into Table
def insert_data_to_table(conn, table_name, df):
    """
    Insert DataFrame data into the specified database table.
    Handles duplicates using ON CONFLICT.
    """
    logger.info(f"Inserting data into table {table_name} with conflict handling...")
    try:
        with conn.cursor() as cur:
            rows = df.to_records(index=False)
            columns = ", ".join(df.columns)
            placeholders = ", ".join(["%s"] * len(df.columns))

            query = f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({placeholders})
            """

            for row in rows:
                cur.execute(query, tuple(row))
        conn.commit()
        logger.info(f"Data successfully inserted/updated into table {table_name}.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting data into {table_name}: {e}")
        raise