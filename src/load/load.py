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

# Constants for environment variables
S3_BUCKET = "banana-squad-processed-data"

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """AWS Lambda handler function."""
    logger.info("Lambda function triggered.")
    try:
        # Extract key from the event
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

# from botocore.exceptions import NoCredentialsError, ClientError
# from load_utils import (
#     connect,
#     create_s3_client,
#     load_parquet,
# )
# import logging

# # Constants
# PROCESSED_BUCKET = "banana-squad-processed-data"

# # Configure logging
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# def lambda_handler(event, context):
#     """
#     Lambda function to load processed data from S3 into the data warehouse.

#     Triggered by S3 Event Notifications.
#     """
#     s3_client = create_s3_client()
#     conn = None

#     try:
#         # Get database connection
#         conn = connect()

#         # Extract bucket name and object key
#         bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
#         object_key = event["Records"][0]["s3"]["object"]["key"]
#         print(object_key)
            
#         logging.info(f"Triggered for file: {object_key} in bucket: {bucket_name}")
            
#         # Determine table name (assuming part of key path)
#         table_name = object_key.split("/")[0]

#         rds_table_name = f"project_team_5.{table_name}"

#         # Load the data into the table
#         load_parquet(s3_client, bucket_name, object_key, rds_table_name, conn)

#     except Exception as e:
#         logging.error(f"Unexpected error: {e}")
#         return {"result": "Failure", "error": str(e)}

#     finally:
#         if conn:
#             conn.close()

#     return {"result": "Success"}