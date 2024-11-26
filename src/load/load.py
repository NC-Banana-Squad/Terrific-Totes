from botocore.exceptions import NoCredentialsError, ClientError
from load_utils import (
    connect,
    create_s3_client,
    load_parquet,
    get_secret
)
import logging
import urllib.parse

# Constants
PROCESSED_BUCKET = "banana-squad-processed-data"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def lambda_handler(event, context):
    """
    Lambda function to load processed data from S3 into the data warehouse.
    Triggered by S3 Event Notifications.
    """
    import json
    logging.info(f"Received event: {json.dumps(event, indent=2)}")  # Log the full event

    s3_client = create_s3_client()
    conn = None

    try:
        # Get database connection
        conn = connect()

        # Extract bucket name and object key
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        object_key = event["Records"][0]["s3"]["object"]["key"]

        # Decode the URL-encoded key
        decoded_key = urllib.parse.unquote(object_key)

        logging.info(f"Extracted bucket_name: {bucket_name}, decoded_key: {decoded_key}")

        # Determine schema and table name from the object key
        table_name = decoded_key.split("/")[0]
        full_table_name = f"project_team_5.{table_name}"

        # Load the data into the table
        load_parquet(s3_client, bucket_name, decoded_key, full_table_name, conn)

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return {"result": "Failure", "error": str(e)}

    finally:
        if conn:
            conn.close()

    return {"result": "Success"}