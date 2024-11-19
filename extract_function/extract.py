from datetime import datetime
from pg8000.exceptions import InterfaceError, DatabaseError
from botocore.exceptions import NoCredentialsError, ClientError
from src.extract.util_functions import (
    connect,
    create_s3_client,
    create_file_name,
    format_to_csv,
    store_in_s3,
)
import logging

data_bucket = "banana-squad-ingested-data"
code_bucket = "banana-squad-code"

logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s"
)

def initial_extract(s3_client, conn):

    """Get public table names from the database"""
    query = conn.run(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name != '_prisma_migrations'"
    )

    """Query each table to extract all information it contains"""
    for table in query:

        file_name = create_file_name(table[0])

        """Create a file like object and keep it in the buffer"""
        rows = conn.run(f"SELECT * FROM {table[0]}")

        columns = [col["name"] for col in conn.columns]

        csv_buffer = format_to_csv(rows, columns)

        """Save the file like object to s3 bucket"""
        try:
            store_in_s3(s3_client, csv_buffer, data_bucket, file_name)
            

        except Exception:
            logging.error(
                f"Failure: the object {file_name} was not created in {data_bucket} bucket"
            )
            return {"result": f"Failed to create an object in {data_bucket} bucket"}
    
    conn.close()
    return {"result": f"Object successfully created in {data_bucket} bucket"}

def continuous_extract(s3_client, conn):

    response = s3_client.get_object(Bucket=code_bucket, Key="last_extracted.txt")
    readable_content = response["Body"].read().decode("utf-8")
    last_extracted_datetime = datetime.fromisoformat(readable_content)
    query = conn.run(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name != '_prisma_migrations'"
    )

    for table in query:
        file_name = create_file_name(table[0])
        rows = conn.run(f"SELECT * FROM {table[0]} WHERE created_at > '{last_extracted_datetime}'")
        columns = [col["name"] for col in conn.columns]

        if rows:
            csv_buffer = format_to_csv(rows, columns)
            store_in_s3(s3_client, csv_buffer, data_bucket, file_name)

    conn.close()
    return {"result": "Success"}

def lambda_handler(event, context):

    try:
        s3_client = create_s3_client()
    except NoCredentialsError:
        logging.error("AWS credentials not found. Unable to create S3 client")
        return {
            "result": "Failure",
            "error": "AWS credentials not found. Unable to create S3 client",
        }
    except ClientError as e:
        logging.error(f"Error creating S3 client: {e}")
        return {"result": "Failure", "error": "Error creating S3 client"}
    
    conn = connect()

    response = s3_client.list_objects(Bucket=code_bucket)
    if "Contents" in response and any(
        obj["Key"] == "last_extracted.txt" for obj in response["Contents"]
    ):
        continuous_extract(s3_client, conn)

    else:
        initial_extract(s3_client, conn)

    try:
        last_extracted = datetime.now().isoformat().replace("T", " ")
        s3_client.put_object(
            Body=last_extracted, Bucket=code_bucket, Key="last_extracted.txt"
        )
    except ClientError as e:
        logging.error(f"Error updating last_extracted.txt: {e}")
        return {"result": "Failure", "error": "Error updating last_extracted.txt"}

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return {"result": "Failure", "error": "Unexpected error"}

    return {"result": "Success"}