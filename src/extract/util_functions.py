from datetime import datetime
import boto3
import csv
import dotenv
import os
import io
import pg8000


def connect():
    """Gets a Connection to the database.
    Credentials are retrieved from environment variables.
    Returns:
        a database connection
    """

    dotenv.load_dotenv()

    user = os.environ["user"]
    database = os.environ["database"]
    password = os.environ["password"]
    host = os.environ["host"]
    port = os.environ["port"]

    return pg8000.connection(
        user=user, database=database, password=password, host=host, port=port
    )

    # return Connection(
    #     user=user, database=database, password=password, host=host, port=port
    # )


def create_s3_client():
    """
    Creates an S3 client using boto3
    """

    return boto3.client("s3")


def create_file_name(table):
    """Function takes a table name provided by either initial or continuous
    extract functions, creates a file system with the parent folder named after the table
    and subsequent folders named after time periods respectively.
    Returns a full file name with a path to it. Path will be created in S3 busket."""

    if not table or not isinstance(table, str):
        table = "UnexpectedQueryErrors"

    year = datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    day = datetime.now().strftime("%d")
    time_now = datetime.now().isoformat()

    file_name = f"{table}/{year}/{month}/{day}/{time_now}.csv"

    return file_name


def format_to_csv(rows, columns):
    """Function receives rows and columns as arguments from either initial or continuous
    extract functions and creates a file like object of csv format in the buffer.
    The pointer in the buffer is reset to the beginning of the file and returns the buffer
    contents, so the file like object can be put into S3 bucket with store_in_s3 function.
    Function allows to avoid potential security breaches that arise when data is saved locally.
    """

    if not columns:
        raise ValueError("Column headers cannot be empty!")

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(columns)
    writer.writerows(rows)

    csv_buffer.seek(0)

    return csv_buffer


def store_in_s3(s3_client, csv_buffer, bucket_name, file_name):
    """
    Uploads a CSV file (in memory) to an AWS S3 bucket.

    Args:
        s3_client: A boto3 S3 client.
        csv_buffer: StringIO object (in-memory file-like) containing CSV data.
        bucket_name (str): The name of the S3 bucket to store the file in.
        file_name (str): The name to assign to the file in the S3 bucket.
    """
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_name)
