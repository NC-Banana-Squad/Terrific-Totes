from datetime import datetime
from pprint import pprint
from pg8000.native import Connection, Error
from pg8000.exceptions import InterfaceError, DatabaseError
from botocore.exceptions import NoCredentialsError, ClientError
# from util_functions import connect, create_s3_client
import logging
import boto3
import csv
import dotenv
import os
import io
import logging

bucket_name = 'banana-squad-code'
logging.basicConfig(level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")

def connect() -> Connection:
    """Gets a Connection to the database.
    Credentials are retrieved from environment variables.
    Returns:
        a database connection   
    """

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

    """
    Creates an S3 client using boto3
    """

    return boto3.client('s3')

def create_file_name(table):

    '''Function takes a table name provided by either initial or continuous
    extract functions, creates a file system with the parent folder named after the table
    and subsequent folders named after time periods respectively. 
    Returns a full file name with a path to it. Path will be created in S3 busket.'''

    if not table or not isinstance(table, str):
        table = 'UnexpectedQueryErrors'

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

    Params: 
        s3_client: A boto3 S3 client.
        csv_buffer: An in-memory file-like object containing CSV data.
        bucket_name (str): The name of the S3 bucket to store the file in.
        file_name (str): The name to assign to the file in the S3 bucket.
    '''

    try:
        if not hasattr(csv_buffer, 'getvalue'):
            raise AttributeError("csv_buffer must be a StringIO object.")
        
        # Attempt to put object into S3
        s3_client.put_object(Body=csv_buffer.getvalue(),
                                Bucket=bucket_name,
                                Key=file_name)
        logging.info(f"Successfully uploaded {file_name} to bucket {bucket_name}")
    
    except ClientError as e:
        err_code = e.response['Error']['Code']
        if err_code == "NoSuchBucket":
            logging.error(f"The specified bucket {bucket_name} does not exist.")
        else:
            logging.error(f"An unexpected ClientError occurred: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected ClientError occurred: {e}")
        raise

def lambda_handler(event, context):

    try:
        s3_client = create_s3_client()
    except NoCredentialsError:
        logger.error("AWS credentials not found. Unable to create S3 client")
        return {"result": "Failure",
                "error": "AWS credentials not found. Unable to create S3 client"}
    except ClientError as e:
        logger.error(f"Error creating S3 client: {e}")
        return {"result": "Failure",
                "error": "Error creating S3 client"}
        
    try:
        response = s3_client.list_objects(Bucket='banana-squad-code')
        if 'Contents' in response and any(obj['Key']== 'last_extracted.txt' for obj in response['Contents']):
            continuous_extract(s3_client)

        # if 'last_extracted.txt' in s3_client.list_objects(Bucket='banana-squad-code'):
        #     continuous_extract(s3_client)

        else:
            initial_extract(s3_client)
    except (InterfaceError, DatabaseError) as e:
        logger.error(f"Error during data extraction: {e}")
        return {"result": "Failure",
                "error": "Error during data extraction"}

    try:
        last_extracted = datetime.now().isoformat().replace('T',' ')
        s3_client.put_object(Body=last_extracted,
                             Bucket='banana-squad-code',
                             Key='last_extracted.txt')
    except ClientError as e:
        logger.error(f"Error updating last_extracted.txt: {e}")
        return {"result": "Failure",
                "error": "Error updating last_extracted.txt"}
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {"result": "Failure",
                "error": "Unexpected error"}

    return {"result": "Success"}
    
def initial_extract():
    '''Create s3 client'''
    try:
        s3_client = create_s3_client() 
    except Exception as e:
        logging.error(f"Failed to create a client from create_client function: {e}")

    '''Connect to database'''
    try:
        conn = connect() 
    except Exception as de:
        logging.error(f"Failed to connect to the database:{de}")

    '''Get public table names from the database'''
    try:    
        query = conn.run('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\' AND table_name != \'_prisma_migrations\'')
    except Exception as sqle:
        logging.error(f"Failed to execute table query:{sqle}")

    '''Query each table to extract all information it contains'''
    for table in query:
        try:
            file_name = create_file_name(table) 
        except Exception as ue:
            logging.error(f"Unexpected error occured: {ue}")

        '''Create a file like object and keep it in the buffer'''  
        rows = conn.run(f'SELECT * FROM {table}')
        columns = [col['name'] for col in conn.columns] 
        
        try:
            csv_buffer = format_to_csv(rows, columns) 
        except Exception as ve:
            logging.error(f"Columns cannot be empty: {ve}")

        '''Save the file like object to s3 bucket'''
        try:
            store_in_s3(s3_client, csv_buffer, bucket_name, file_name)
            return {"result": f"Object successfully created in {bucket_name} bucket"}
        
        except Exception:
            logging.error(f"Failure: the object {file_name} was not created in {bucket_name} bucket")
            return {"result": f"Failed to create an object in {bucket_name} bucket"}

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

pprint(connect())