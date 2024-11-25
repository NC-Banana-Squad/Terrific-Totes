from datetime import datetime
from pg8000.exceptions import InterfaceError, DatabaseError
from botocore.exceptions import NoCredentialsError, ClientError
from util_functions import (
    connect,
    create_s3_client,
    create_file_name,
    format_to_csv,
    store_in_s3,
)
import logging
import json

data_bucket = "banana-squad-ingested-data"
code_bucket = "banana-squad-code"

logging.basicConfig(
    level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s"
)


def initial_extract(s3_client, conn):
    """
    Function to run an initial extract of all data currently in the ToteSys database and stores in an S3 bucket.
    - runs query to find all table names in db
    - creates file name using create_file_name util function
    - runs query to select all data from each table
    - converts data to csv format
    - stores csv file in S3 bucket

        Parameters:
            s3_client: a low-level interface for interacting with S3 buckets
            conn: a connection to the ToteSys database

        Returns: string declaring success or failure of upload to S3
    """

    query = conn.run(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name != '_prisma_migrations'"
    )
    extracted_tables = []
    """Query each table to extract all information it contains"""
    for table in query:

        file_name = create_file_name(table[0])
        rows = conn.run(f"SELECT * FROM {table[0]}")
        columns = [col["name"] for col in conn.columns]

        if rows:
            csv_buffer = format_to_csv(rows, columns)
            store_in_s3(s3_client, csv_buffer, data_bucket, file_name)
            extracted_tables.append(file_name)

    return extracted_tables


def continuous_extract(s3_client, conn):
    """
    Function to run an extract of recently added data in the ToteSys db and stores in an S3 bucket.
    - reads timestamp stored in last_extracted.txt
    - runs db query to get all table names from db
    - runs a db query to select all new data added since timestamp
    - creates file name with create_file_name util function
    - converts data into csv format
    - stores csv file in S3 bucket

        Parameters:
            s3_client: a low-level interface for interacting with S3 buckets
            conn: a connection to the ToteSys database

        Returns: string declaring success or failure of upload to S3
    """

    response = s3_client.get_object(Bucket=code_bucket, Key="last_extracted.txt")
    readable_content = response["Body"].read().decode("utf-8")
    last_extracted_datetime = datetime.fromisoformat(readable_content)
    query = conn.run(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name != '_prisma_migrations'"
    )
    updated_tables = []
    for table in query:
        file_name = create_file_name(table[0])
        rows = conn.run(
            f"SELECT * FROM {table[0]} WHERE created_at > '{last_extracted_datetime}'"
        )
        print(f"Rows for table {table[0]}: {rows}")
        columns = [col["name"] for col in conn.columns]

        if rows:
            csv_buffer = format_to_csv(rows, columns)
            store_in_s3(s3_client, csv_buffer, data_bucket, file_name)
            updated_tables.append(file_name)

    print(f"Returning {updated_tables}")
    return updated_tables


def lambda_handler(event, context):
    """
    Function contains logic to extract data from ToteSys database based on whether an initial extract has taken place or not.
    - creates an S3 client to interact with S3 bucket.
    - creates a connection to the ToteSys database
    - checks whether a 'last_extracted.txt' exists in AWS and invokes either 'initial_extract' or 'continuous_extract' accordingly
    - creates (after initial_extract) OR updates (after continuous_extract)a file called 'last_extracted.txt' and uploads to S3
    - closes connection

        Parameters:
            s3_client: a low-level interface for interacting with S3 buckets
            conn: a connection to the ToteSys database

        Returns: string declaring success or failure
    """

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
        result = continuous_extract(s3_client, conn)
        extraction_type = "Continuous"

    else:
        result = initial_extract(s3_client, conn)
        extraction_type = "Initial"

    # try:
    last_extracted = datetime.now().isoformat().replace("T", " ")
    s3_client.put_object(
        Body=last_extracted, Bucket=code_bucket, Key="last_extracted.txt"
    )

    report = {
        "status": "Success",
        "extraction_type": extraction_type,
        "timestamp": last_extracted,
        "updated_tables": result,
    }

    report_file_name = (
        f"reports/{extraction_type}_extract_{last_extracted}_success.json"
    )
    s3_client.put_object(
        Body=json.dumps(report, indent=4),
        Bucket=data_bucket,
        Key=report_file_name,
    )

    conn.close()

    return {
        "result": "Success",
        "report_file": f"s3://{code_bucket}/{report_file_name}",
    }
    # except ClientError as e:
    #     logging.error(f"Error updating last_extracted.txt: {e}")
    #     return {"result": "Failure", "error": "Error updating last_extracted.txt"}

    # except Exception as e:
    #     logging.error(f"Unexpected error: {e}")
    #     return {"result": "Failure", "error": "Unexpected error"}

    # finally:    
