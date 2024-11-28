import os
import logging
from load_utils import (
    connect,
    create_s3_client,
    load_parquet_from_s3,
    insert_data_to_table
)
from botocore.exceptions import ClientError
from urllib.parse import unquote

S3_BUCKET = "banana-squad-processed-data"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Function contains logic to load data from Processed Bucket into RDS Postgres database
    Logs errors to CloudWatch for ease of maintenance

        Parameters:
            triggered upon addition of a parquet file to the Processed Bucket
            event contains information about added parquet file

    """    
    logger.info("Lambda function triggered.")
    try:
        s3_key = event["Records"][0]["s3"]["object"]["key"]
        decoded_key = unquote(s3_key)
        logger.info(f"Processing S3 key: {decoded_key}")
        table_name = decoded_key.split("/")[0]

        s3_client = create_s3_client()
        conn = connect()

        try:
            df = load_parquet_from_s3(s3_client, S3_BUCKET, decoded_key)
            valid_tables = [
                "fact_sales_order",
                "dim_staff",
                "dim_counterparty",
                "dim_location",
                "dim_date",
                "dim_currency",
                "dim_design"
            ]
            if table_name not in valid_tables:
                logger.error(f"Unexpected table name: {table_name}")
                raise ValueError(f"Unexpected table name: {table_name}")

            insert_data_to_table(conn, table_name, df)

        finally:
            conn.close()
            logger.info("Database connection closed.")

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f"File not found: {decoded_key} in bucket {S3_BUCKET}")
        else:
            logger.error(f"ClientError: {e}")
        raise
    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        raise