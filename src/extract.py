from pg8000.native import Connection
import dotenv
import os
from datetime import datetime
import logging
import boto3
from pprint import pprint
import csv
#from botocore import botocore


s3_client = boto3.client('s3')

def connect():
    dotenv.load_dotenv()

    # cohort_id = os.environ['cohort_id']
    user = os.environ['user']
    database = os.environ['database']
    password = os.environ['password']
    host = os.environ['host']
    port = os.environ['port']

    return Connection(

        user=user,
        database=database,
        password=password,
        host=host,
        port=port
    )

def fetch():
    conn = connect()
    rows = conn.run('SELECT * FROM sales_order')
    columns = [col['name'] for col in conn.columns]


    with open('data_fetch.csv', 'w', encoding='utf-8') as csv_f:
        writer = csv.writer(csv_f)
        writer.writerow(columns)
        writer.writerows(rows)
    

fetch()



def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    conn = connect()
    rows = conn.run('SELECT * FROM sales_order')
    columns = [col['name'] for col in conn.columns]

    #table_name = [name for name in tables]
    year = datetime.now().strftime('%Y')
    month = datetime.now().strftime('%m')
    day = datetime.now().strftime('%d')
    time_now = datetime.now().isoformat()
    file_name = f'{year}/{month}/{day}/{time_now}.csv'
    try:
        json_data = json.dumps(event)
        s3_client.put_object(Body=json_data, Bucket=data_bucket_name, Key=file_name)
        return {"result": "Success"}
    except Exception:
        return {"result": "Failure"}

