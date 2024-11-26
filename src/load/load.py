from botocore.exceptions import NoCredentialsError, ClientError
from load_utils import (
    connect,
    create_s3_client,
    load_parquet,
    create_table,
    get_secret
)
import logging

# Constants
PROCESSED_BUCKET = "banana-squad-processed-data"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def lambda_handler(event, context):
    """
    Lambda function to load processed data from S3 into the data warehouse.
    Triggered by S3 Event Notifications.
    """
    s3_client = create_s3_client()
    conn = None

    try:
        # Get database connection
        conn = connect()

        # Extract bucket name and object key
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]
            
        logging.info(f"Triggered for file: {object_key} in bucket: {bucket_name}")
            
        # Determine table name (assuming part of key path)
        table_name = object_key.split("/")[0]

        # Load the data into the table
        load_parquet(s3_client, bucket_name, object_key, table_name, conn)

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return {"result": "Failure", "error": str(e)}

    finally:
        if conn:
            conn.close()

    return {"result": "Success"}