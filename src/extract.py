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
from botocore.exceptions import ClientError
import logging

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

    """
    Add comment :)
    """
    return boto3.client('s3')

def create_file_name(table):

    '''Function takes a table name provided by either initial or continuous
    extract functions, creates a file system with the parent folder named after the table
    and subsequent folders named after time periods respectively. 
    Returns a full file name with a path to it. Path will be created in S3 busket.'''

    if not table or not isinstance(table, str):
        raise ValueError("Table name cannot be empty!")

    year = datetime.now().strftime('%Y')
    month = datetime.now().strftime('%m')
    day = datetime.now().strftime('%d')
    time_now = datetime.now().isoformat()

    file_name = f'{table}/{year}/{month}/{day}/{time_now}.csv'

    return file_name

def format_to_csv(rows, columns):

    '''Function receives rows and columns as arguments from either initial or continuous
    extract functions and creates a file like object of csv format in the buffer.
    The pointer in the buffer is reset to the beginning of the file and returns the buffer
    contents, so the file like object can be put into S3 bucket with store_in_s3 function.
    Function allows to avoid potential security breaches that arise when data is saved locally.'''

    if not columns:
        raise ValueError('Column headers cannot be empty!')
    
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(columns)
    writer.writerows(rows)

    csv_buffer.seek(0)

    return csv_buffer


def store_in_s3(s3_client, csv_buffer, bucket_name, file_name):
    '''
    Uploads a CSV file (in memory) to an AWS S3 bucket.

    Args: 
        s3_client: A boto3 S3 client.
        csv_buffer: StringIO object (in-memory file-like) containing CSV data.
        bucket_name (str): The name of the S3 bucket to store the file in.
        file_name (str): The name to assign to the file in the S3 bucket.
    '''
    s3_client.put_object(Body=csv_buffer.getvalue(),
                                Bucket=bucket_name,
                                Key=file_name)
    
    # Code with try/except blocks and errors raised
    # logging.info(f"Successfully uploaded {file_name} to bucket {bucket_name}")
    # if not bucket_name or not file_name:
    #     raise ValueError("Bucket name and file name must not be empty") 
    # if not hasattr(csv_buffer, 'getvalue'):
    #     raise ValueError("csv_buffer must be a StringIO object.")
    # try:
        # Attempt to put object into S3
        # s3_client.put_object(Body=csv_buffer.getvalue(),
    #                             Bucket=bucket_name,
    #                             Key=file_name)
    # logging.info(f"Successfully uploaded {file_name} to bucket {bucket_name}")
    # except ClientError as ce:
    #     error_code = ce.response['Error']['Code']
    #     if error_code == "NoSuchBucket":
    #         logging.error(f"The specified bucket {bucket_name} does not exist.")
    #         raise
    #     elif error_code == "AccessDenied":
    #         logging.error(f"Access denied when attempting to upload {file_name} to bucket {bucket_name}. Please check your IAM permissions.")
    #         raise
    #     else:
    #         logging.error(f"An unexpected ClientError occurred: {ce}")
    # except Exception as e:
    #     logging.error(f"An unexpected ClientError occurred: {e}")
    #     raise


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
    
    response = s3_client.get_object(Bucket=bucket_name, Key='last_extracted.txt')
    readable_content = response['Body'].read().decode('utf-8')
    
    query = conn.run('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\' AND table_name != \'_prisma_migrations\'')
    
    for table in query:    
        file_name = create_file_name(table)
        rows = conn.run(f'SELECT * FROM {table} WHERE created_at > {readable_content}')
        columns = [col['name'] for col in conn.columns]
        
        if rows:
            csv_buffer = format_to_csv(rows, columns)
            store_in_s3(s3_client, csv_buffer, bucket_name, file_name)
    
    conn.close()
    return {"result": "Success"}


    # s3_client = create_s3_client()
    # conn = connect()
    # response = s3_client.get_object(Bucket='banana-squad-code', Key='last_extracted.txt')
    # contents = response['Body'].read()
    # readable_content = contents.decode('utf-8')
    # query = conn.run('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\' AND table_name != \'_prisma_migrations\'')

    # for table in query:    
    #     file_name = create_file_name(table)
    #     rows = conn.run(f'SELECT * FROM {table} WHERE created_at > {readable_content}')
    #     columns = [col['name'] for col in conn.columns]

    #     if rows:

    #         csv_buffer = format_to_csv(rows, columns)

    #         try:
    #             store_in_s3(s3_client, csv_buffer, bucket_name, file_name)
    #             return {"result": "Success"}
    #         except Exception:
    #             return {"result": "Failure"}
        
    # conn.close()