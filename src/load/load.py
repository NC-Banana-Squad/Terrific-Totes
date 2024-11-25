from botocore.exceptions import NoCredentialsError, ClientError
from load_utils import (
    connect,
    create_s3_client,
    load_parquet,
    create_table
)
import logging

# Constants
PROCESSED_BUCKET = "banana-squad-processed-data"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def lambda_handler(event, context):
    """
    Lambda function to load processed data from S3 into the data warehouse.
    """
    s3_client = create_s3_client()
    conn = None

    try:
        # Get database connection
        conn = connect()

        # List all files in the processed S3 bucket
        response = s3_client.list_objects(Bucket=PROCESSED_BUCKET)
        if "Contents" not in response:
            logging.info("No files found in the processed bucket.")
            return
        
        # Load the data into the corresponding table
        load_parquet()
       


    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return {"result": "Failure", "error": "Unexpected error"}

    finally:
        if conn:
            conn.close()     