from datetime import datetime
from pg8000.exceptions import InterfaceError, DatabaseError
from botocore.exceptions import NoCredentialsError, ClientError
import logging
import pandas as pd
import boto3
import io
import urllib.parse
from transform_utils import transform_sales_order_utils


def lambda_handler(event, context):
    s3_client = boto3.client("s3", region_name='eu-west-2')
    #Below variables use s3 trigger to read info from the event argument. This gets bucket name and key name of recent s3 update
    bucket = event['Records'][0]['s3']['bucket']['name'] #hardcode bucket name?
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    obj = s3_client.get_object(Bucket=bucket, Key=key)

    #reads the botocore file stream
    file_stream = io.StringIO(obj['Body'].read().decode('utf-8'))
    #reads csv and makes a df. Loads data into df
    df = pd.read_csv(file_stream)
    table_names = {"sales_order": "fact_sales_order", #implement conditional logic for if get()returns a list
                   "address": "dim_location", 
                   "staff": "dim_staff", 
                   "currency": "dim_currency",
                   "design": "dim_design",
                   "counterparty": "dim_counterparty"
                }
    
    function_to_invoke = key.split("/")[0] #table name really
    table_name = table_names.get(function_to_invoke)
    year = key.split("/")[1]
    month = key.split("/")[2]
    day = key.split("/")[3]
    filename = key.split("/")[4]
    path = f'{table_name}/{year}/{month}/{day}/{filename}'

    result_table = f"{function_to_invoke(df)}" #do we need it to be a string?
    parquet_buffer = io.BytesIO()
    result_table.to_parquet(parquet_buffer, index=False)

    #dim_date()

    s3_client.put_object(Body=parquet_buffer.getvalue(), Bucket='banana-squad-processed-data', Key=path)

    #key is the filename just updated:

    #'sales_order/2024/11/20/timestamp.csv'
    
    #Pseudocode for transform. 
    #Call a function which takes the df above as an argument. The function called depends on the file/table name read. Get this from key.  

    # if key.split('/')[0] == 'sales_order':
    #     transformed_data = transform_sales_util(df)

    #elif for each table and its regarding util function. 

    #Once table conditions and transfrom complete. Convert to parquet

    # transformed_data to parquet. Can use Pandas df.to_parquet()
    # parquet_data, upload to processed_bucket with correct file name. Maybe using key again.
    # s3_client.put_object(Body=parquet_data.csv, Bucket='banana-squad-processed-data', Key=key)