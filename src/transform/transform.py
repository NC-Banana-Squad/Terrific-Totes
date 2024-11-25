import pandas as pd
import boto3
import io
import urllib.parse
import json
from transform_utils import fact_sales_order, dim_staff, dim_counterparty, dim_location, dim_currency, dim_date, dim_design


def get_data_frame(s3_client, bucket="banana-squad-ingested-data", key=None): #check what is the key it needs
    """Fetches and returns a DataFrame from an S3 bucket."""
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    file_stream = io.StringIO(obj['Body'].read().decode('utf-8'))
    return pd.read_csv(file_stream)

def lambda_handler(event, context):
    s3_client = boto3.client("s3", region_name='eu-west-2')

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    success_data = json.loads(obj['Body'].read().decode('utf-8'))

    updated_tables = success_data.get("updated_tables", [])
    transformations = {
        fact_sales_order : ["sales_order"],
        dim_counterparty : ["counterparty", "address"],
        dim_currency: ["currency"],
        dim_location: ["address"],
        dim_design: ["design"],
        dim_staff: ["staff", "department"]
    }

    data_frames = []
    for table in updated_tables:
        print(table)
        df = get_data_frame(s3_client, bucket, table)
        df.name = table.split("/")[0]
        data_frames.append(df)
        # except Exception as e:
        #     raise ValueError(f"Error loading source data frame {source}: {e}")
    

    for table in updated_tables:
        table_name = table.split("/")[0]
        for transform_function, sources in transformations.items():
            if table_name in sources:  # Check if table_name exists in the sources
                relevant_data_frames = [
                df for df in data_frames if df.name.split("/")[0] in sources
            ]
                result_table = transform_function(*relevant_data_frames)  # Call the function
                break

    # Write the resulting data frame to the processed bucket in Parquet format
            parquet_buffer = io.BytesIO()
            result_table.to_parquet(parquet_buffer, index=False)
            file_name = f"{'/'.join(table.split('/')[1:4])}/{table.split('/')[4][:-4]}"
            # year, month, day, filename = table.split('/')[1:5]
            output_path = f"{transform_function.__name__}{file_name}.parquet"
            s3_client.put_object(Body=parquet_buffer.getvalue(), Bucket="banana-squad-processed-data", Key=output_path)

    response = s3_client.list_objects(Bucket="banana-squad-processed-data")
    if "Contents" in response and any(
        obj["Key"] == "dim_date/2024/11/25/15:00:00.00000.parquet" for obj in response["Contents"]
    ):
        date_table = dim_date()
        parquet_buffer = io.BytesIO()
        date_table.to_parquet(parquet_buffer, index=False)
        output_path = "dim_date/2024/11/25/15:00:00.00000.parquet"
        s3_client.put_object(Body=parquet_buffer.getvalue(), Bucket="banana-squad-processed-data", Key=output_path)
    
    return "Transformation completed"