from datetime import datetime
from pprint import pprint
from pg8000.native import Connection
#from util_functions import connect, create_s3_client
import boto3
import csv
import dotenv
import os
import io

bucket_name = 'banana-squad-code'

def connect():
    dotenv.load_dotenv()

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

def create_s3_client():
    return boto3.client('s3')

def create_file_name(table):

    year = datetime.now().strftime('%Y')
    month = datetime.now().strftime('%m')
    day = datetime.now().strftime('%d')
    time_now = datetime.now().isoformat()

    file_name = f'{table}/{year}/{month}/{day}/{time_now}.csv'

    return file_name

def format_to_csv(rows, columns):

    if not columns:
        raise ValueError('Column headers cannot be empty!')
    
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(columns)
    writer.writerows(rows)

    csv_buffer.seek(0)

    return csv_buffer


def store_in_s3(s3_client, csv_buffer, bucket_name, file_name):
    s3_client.put_object(Body=csv_buffer.getvalue(),
                         Bucket=bucket_name,
                         Key=file_name)

def lambda_handler(event, context):
    s3_client = create_s3_client()
    if 'last_extracted.txt' in s3_client.list_objects(Bucket='banana-squad-code'):
        continuous_extract()
    else:
        initial_extract()

    last_extracted = datetime.now().isoformat().replace('T',' ')
    s3_client.put_object(Body=last_extracted, 
                                 Bucket='banana-squad-code', 
                                 Key='last_extracted.txt')
    
def initial_extract():    
    s3_client = create_s3_client()
    conn = connect()
    query = conn.run('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\' AND table_name != \'_prisma_migrations\'')

    for table in query:

        file_name = create_file_name(table)
        rows = conn.run(f'SELECT * FROM {table}')
        columns = [col['name'] for col in conn.columns]

        csv_buffer = format_to_csv(rows, columns)

        try:
            store_in_s3(s3_client, csv_buffer, bucket_name, file_name)
            return {"result": "Success"}
        
        except Exception:
            return {"result": "Failure"}

    conn.close()
  
def continuous_extract():
    s3_client = create_s3_client()
    conn = connect()
    response = s3_client.get_object(Bucket='banana-squad-code', Key='last_extracted.txt')
    contents = response['Body'].read()
    readable_content = contents.decode('utf-8')
    query = conn.run('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\' AND table_name != \'_prisma_migrations\'')

    for table in query:    
        file_name = create_file_name(table)
        rows = conn.run(f'SELECT * FROM {table} WHERE created_at > {readable_content}')
        columns = [col['name'] for col in conn.columns]

        if rows:

            csv_buffer = format_to_csv(rows, columns)

            try:
                store_in_s3(s3_client, csv_buffer, bucket_name, file_name)
                return {"result": "Success"}
            except Exception:
                return {"result": "Failure"}
        
    conn.close()