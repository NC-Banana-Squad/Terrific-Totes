from pg8000.native import Connection
import dotenv
import os
from datetime import datetime
import logging
import boto3
from pprint import pprint
import csv
import io


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


# def fetch():
#     conn = connect()
#     rows = conn.run('SELECT * FROM sales_order')
#     columns = [col['name'] for col in conn.columns]


#     with open('data_fetch.csv', 'w', encoding='utf-8') as csv_f:
#         writer = csv.writer(csv_f)
#         writer.writerow(columns)
#         writer.writerows(rows)
    

# fetch()



def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    if 'last_extracted.txt' in s3_client.list_objects(Bucket='banana-squad-code'):
        continuous_extract()
    else:
        initial_extract()

    last_extracted = datetime.now().isoformat().replace('T',' ')
    s3_client.put_object(Body=last_extracted, 
                                 Bucket='banana-squad-code', 
                                 Key='last_extracted.txt')
    


def initial_extract():    
    s3_client = boto3.client('s3')
    conn = connect()
    query = conn.run('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\' AND table_name != \'_prisma_migrations\'')
    # all_tables = conn.run('SELECT tables FROM information_')

    table_name = [name for name in query]
    year = datetime.now().strftime('%Y')
    month = datetime.now().strftime('%m')
    day = datetime.now().strftime('%d')
    time_now = datetime.now().isoformat()
    file_name = f'{table_name}/{year}/{month}/{day}/{time_now}.csv'

    for table in query:
        rows = conn.run(f'SELECT * FROM {table}')
        columns = [col['name'] for col in conn.columns]

        csv_buffer= io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(columns)
        writer.writerows(rows)

        csv_buffer.seek(0)

        try:
            s3_client.put_object(Body=csv_buffer.getvalue(), 
                                 Bucket='banana-squad-code', 
                                 Key=file_name)
            return {"result": "Success"}
        except Exception:
            return {"result": "Failure"}

    conn.close()
   
def continuous_extract():
    s3_client = boto3.client('s3')
    conn = connect()
    response = s3_client.get_object(Bucket='banana-squad-code', Key='last_extracted.txt')
    contents = response['Body'].read()
    readable_content = contents.decode('utf-8')
    query = conn.run('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\' AND table_name != \'_prisma_migrations\'')
    table_name = [name for name in query]

    for table in table_name:
        query = conn.run('SELECT created_at FROM sales_order WHERE created_at >')

# time = datetime.now().isoformat().replace('T',' ')
# print(time)

# s3_client = boto3.client('s3')
# conn = connect()
# query = conn.run('SELECT created_at, last_updated FROM sales_order WHERE created_at != last_updated')
# print(query)