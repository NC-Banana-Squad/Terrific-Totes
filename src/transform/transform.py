import pandas as pd
import boto3
import io
import urllib.parse
import json
import logging
from transform_utils import fact_sales_order, dim_staff, dim_counterparty, dim_location, dim_currency, dim_date, dim_design

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_data_frame(s3_client, bucket, key):
    """Fetches and returns a DataFrame from an S3 bucket."""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        file_stream = io.StringIO(obj['Body'].read().decode('utf-8'))
        return pd.read_csv(file_stream)
    except Exception as e:
        print(f"Error loading DataFrame for key '{key}': {e}")
        return None


def lambda_handler(event, context):
    s3_client = boto3.client("s3", region_name='eu-west-2')

    # Extract bucket and key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # Load the JSON content from the S3 object
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    success_data = json.loads(obj['Body'].read().decode('utf-8'))

    updated_tables = success_data.get("updated_tables", [])
    transformations = {
        fact_sales_order: ["sales_order"],
        dim_counterparty: ["counterparty", "address"],
        dim_currency: ["currency"],
        dim_location: ["address"],
        dim_design: ["design"],
        dim_staff: ["staff", "department"]
    }

    # Load DataFrames and handle errors
    data_frames = {}
    for table in updated_tables:
        df = get_data_frame(s3_client, bucket, table)
        if df is not None:
            df.name = table.split("/")[0]
            data_frames[df.name] = df
        else:
            print(f"Skipping table '{table}' due to load error.")

    # Apply transformations
    processed_files = set()  # Track files to prevent duplicates
    for transform_function, sources in transformations.items():
        relevant_data_frames = [
            data_frames[source] for source in sources if source in data_frames
        ]
        if relevant_data_frames:
            result_table = transform_function(*relevant_data_frames)

            # Generate a unique directory name
            year, month, day, file_name = table.split('/')[1:5]
            output_path = f"{transform_function.__name__}/{year}/{month}/{day}/{file_name[:-4]}.parquet"

            if output_path not in processed_files:
                processed_files.add(output_path)

                # Write the transformed DataFrame to the processed bucket
                parquet_buffer = io.BytesIO()
                result_table.to_parquet(parquet_buffer, index=False)

                s3_client.put_object(Body=parquet_buffer.getvalue(), Bucket="banana-squad-processed-data", Key=output_path)
                print(f"Processed and uploaded file: {output_path}")

    # Check for dim_date table existence and process it
    response = s3_client.list_objects(Bucket="banana-squad-processed-data")
    if "Contents" in response and not any(
        obj["Key"] == "dim_date/2024/11/25/15:00:00.00000.parquet" for obj in response["Contents"]
    ):
        date_table = dim_date()
        parquet_buffer = io.BytesIO()
        date_table.to_parquet(parquet_buffer, index=False)
        output_path = "dim_date/2024/11/25/15:00:00.00000.parquet"
        s3_client.put_object(Body=parquet_buffer.getvalue(), Bucket="banana-squad-processed-data", Key=output_path)
        print(f"Processed and uploaded dim_date table: {output_path}")

    conn.close()
    return "Transformation completed"