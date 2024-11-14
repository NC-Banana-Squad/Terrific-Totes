from datetime import datetime
from pprint import pprint
from pg8000.exceptions import InterfaceError, DatabaseError
from botocore.exceptions import NoCredentialsError, ClientError
from util_functions import connect, create_s3_client, create_file_name, format_to_csv,store_in_s3
import logging

bucket_name = 'banana-squad-code'
logging.basicConfig(level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")
    
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

def lambda_handler(event, context):

    try:
        s3_client = create_s3_client()
    except NoCredentialsError:
        logging.error("AWS credentials not found. Unable to create S3 client")
        return {"result": "Failure",
                "error": "AWS credentials not found. Unable to create S3 client"}
    except ClientError as e:
        logging.error(f"Error creating S3 client: {e}")
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
        logging.error(f"Error during data extraction: {e}")
        return {"result": "Failure",
                "error": "Error during data extraction"}

    try:
        last_extracted = datetime.now().isoformat().replace('T',' ')
        s3_client.put_object(Body=last_extracted,
                             Bucket='banana-squad-code',
                             Key='last_extracted.txt')
    except ClientError as e:
        logging.error(f"Error updating last_extracted.txt: {e}")
        return {"result": "Failure",
                "error": "Error updating last_extracted.txt"}
    
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return {"result": "Failure",
                "error": "Unexpected error"}

    return {"result": "Success"}