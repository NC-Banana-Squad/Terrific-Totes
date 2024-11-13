from datetime import datetime
from pprint import pprint
from pg8000.native import Connection, Error
from pg8000.exceptions import InterfaceError, DatabaseError
# from util_functions import connect, create_s3_client
import logging
import boto3
import csv
import dotenv
import os
import io

bucket_name = 'banana-squad-code'

# Giving logger the name of the module in which it is used (lambda_handler)
logger = logging.getLogger(__name__)

# '-> Connection' syntax just tells us this function returns a Connection
def connect() -> Connection:
    """Gets a Connection to the database.

    Credentials are retrieved from environment variables.

    Returns:
        a database connection

    Raises:
        DBConnectionException
    """

    dotenv.load_dotenv()

    try:

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

    # Handles missing environment variables    
    except KeyError as e:
        logger.error(f"Missing environment variable: {e}")
        raise KeyError(f"Missing environment variable: {e}")

    # Handles connection issues (wrong credentials, network problems etc)
    except InterfaceError as e:
        logger.error(f"Database interface error: {e}")
        raise InterfaceError(f"Database interface error: {e}")

    # Handles errors related to database instructions
    except DatabaseError as e:
        logger.error(f"Failed to connect to db: {e}")
        raise DatabaseError(f"Failed to connect to db: {e}")
    
    # Handles general pg8000 errors
    except Error as e:
        logger.error(f"pg8000 error: {e}")
        raise Error(f"pg8000 error: {e}")
    
    # Catches any other general errors that could arise    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise Exception(f"Unexpected error: {e}")

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