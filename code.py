import boto3
import pandas as pd
import io
import logging
import json
from botocore.exceptions import ClientError
# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# Constants
INGESTION_BUCKET = "banana-squad-ingested-data"
PROCESSED_BUCKET = "banana-squad-processed-data"

# Helper Functions
def transform_data(dataframe, table_name):
    """
    Apply transformations to the ingested data to conform to the warehouse schema.
    Args:
        dataframe (pd.DataFrame): The data to be transformed.
        table_name (str): The name of the source table.
    Returns:
        pd.DataFrame: The transformed data.
    """
    try:
        if table_name == "counterparty":
            dataframe = dataframe.rename(columns={
                "legal_name": "counterparty_legal_name",
                "address_line_1": "counterparty_legal_address_line_1",
                "address_line_2": "counterparty_legal_address_line_2",
                "district": "counterparty_legal_district",
                "city": "counterparty_legal_city",
                "postal_code": "counterparty_legal_postal_code",
                "country": "counterparty_legal_country",
                "phone_number": "counterparty_legal_phone_number",
            })
        elif table_name == "staff":
            dataframe = dataframe.rename(columns={
                "name": "first_name",
                "surname": "last_name",
                "dept_name": "department_name",
                "email": "email_address",
            })
        elif table_name == "currency":
            dataframe = dataframe.rename(columns={
                "code": "currency_code",
                "name": "currency_name",
            })
            
        # Add transformations for other tables here as needed
        # Example transformation: Ensure all column names are lowercase
        dataframe.columns = [col.lower() for col in dataframe.columns]
        # Example validation: Check for required fields
        if "id" in dataframe.columns:
            dataframe = dataframe.dropna(subset=["id"])  # Drop rows with missing IDs
        return dataframe
    except Exception as e:
        logging.error(f"Error during transformation for table {table_name}: {e}")
        raise
    
def upload_to_s3_parquet(s3_client, dataframe, bucket_name, key):
    """
    Upload the transformed DataFrame as a Parquet file to the S3 bucket.
    Args:
        s3_client: Boto3 S3 client instance.
        dataframe (pd.DataFrame): Transformed data.
        bucket_name (str): The S3 bucket name.
        key (str): The S3 object key.
    """
    try:
        parquet_buffer = io.BytesIO()
        dataframe.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        parquet_buffer.seek(0)
        s3_client.put_object(Body=parquet_buffer.getvalue(), Bucket=bucket_name, Key=key)
        logging.info(f"File successfully uploaded to S3 as Parquet: {bucket_name}/{key}")
    except ClientError as e:
        logging.error(f"Failed to upload file to S3: {e}")
        raise

# Main Lambda Handler
def lambda_handler(event, context):
    """
    Lambda function triggered by S3 to transform ingested data and upload it to the processed bucket.
    Args:
        event: AWS Lambda event object.
        context: AWS Lambda context object.
    """
    s3_client = boto3.client("s3")
    try:
        # Parse S3 event
        for record in event["Records"]:
            bucket_name = record["s3"]["bucket"]["name"]
            object_key = record["s3"]["object"]["key"]
            logging.info(f"Triggered for file: {bucket_name}/{object_key}")
            # Get the object from S3
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            file_content = response["Body"].read().decode("utf-8")
            # Load the data into a pandas DataFrame
            dataframe = pd.read_csv(io.StringIO(file_content))
            table_name = object_key.split("/")[0]  # Assuming table name is in the path
            # Transform the data
            transformed_data = transform_data(dataframe, table_name)
            # Create the processed file path
            processed_key = object_key.replace("ingested-data", "processed-data").replace(".csv", ".parquet")
            # Upload the transformed data to the processed S3 bucket as Parquet
            upload_to_s3_parquet(s3_client, transformed_data, PROCESSED_BUCKET, processed_key)
        return {"statusCode": 200, "body": json.dumps("Transformation complete.")}
    except Exception as e:
        logging.error(f"Error in Lambda handler: {e}")
        return {"statusCode": 500, "body": json.dumps("Transformation failed.")}