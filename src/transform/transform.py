import pandas as pd
import boto3
import io
import urllib.parse
from transform_utils import sales_order, counterparty, currency, date, design, address, staff

def get_data_frame(s3_client, bucket, key):
    """Fetches and returns a DataFrame from an S3 bucket."""
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    file_stream = io.StringIO(obj['Body'].read().decode('utf-8'))
    return pd.read_csv(file_stream)

def lambda_handler(event, context):
    s3_client = boto3.client("s3", region_name='eu-west-2')

    # Define mappings for target tables, source data frames, and transformation functions
    transformations = {
        "fact_sales_order": {
            "sources": ["sales_order"],
            "function": sales_order
        },
        "dim_counterparty": {
            "sources": ["counterparty", "address"],
            "function": counterparty
        },
        "dim_currency": {
            "sources": ["currency"],
            "function": currency
        },
        "dim_date": {
            "sources": ["date"],
            "function": date
        },
        "dim_location": {
            "sources": ["address"],
            "function": address
        },
        "dim_design": {
            "sources": ["design"],
            "function": design
        },
        "dim_staff": {
            "sources": ["staff", "department"],
            "function": staff
        }
    }

    # Extract bucket and key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # Determine which table to transform based on the folder structure
    target_table = key.split("/")[0]
    year, month, day, filename = key.split("/")[1:5]

    if target_table not in transformations:
        raise ValueError(f"Unknown target table: {target_table}")

    # Load all required source data frames
    sources = transformations[target_table]["sources"]
    data_frames = []
    for source in sources:
        source_key = f"{source}/{year}/{month}/{day}/{filename}"
        try:
            df = get_data_frame(s3_client, bucket, source_key)
            data_frames.append(df)
        except Exception as e:
            raise ValueError(f"Error loading source data frame {source}: {e}")

    # Apply the transformation function
    transform_function = transformations[target_table]["function"]
    result_table = transform_function(*data_frames)

    # Write the resulting data frame to the processed bucket in Parquet format
    parquet_buffer = io.BytesIO()
    result_table.to_parquet(parquet_buffer, index=False)
    output_path = f"{target_table}/{year}/{month}/{day}/{filename}"
    s3_client.put_object(Body=parquet_buffer.getvalue(), Bucket="banana-squad-processed-data", Key=output_path)

