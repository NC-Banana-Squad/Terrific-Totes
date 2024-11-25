import boto3
from io import BytesIO
from pg8000.native import Connection


# Constants
PROCESSED_BUCKET = "banana-squad-processed-data"

def get_secret(secret_name, region_name=None):
    """
    Retrieves a secret from AWS Secrets Manager

    Args:
        secret_name (str): The name of the secret in Secrets Manager
    Returns:
        dict: A dictionary of the secret values.
    """
    region_name = "eu-west-2"

    client = boto3.client("secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
            return json.loads(secret)
        else:
            raise ValueError("Secret is stored as binary; function expects JSON.")
    except Exception as e:
        raise RuntimeError("Failed to retrieve secret: {e}")

def connect():
    """Gets a Connection to the ToteSys database.
    Credentials are retrieved from AWS Secrets Manager by invoking get_secrets().
    Returns:
        a connection to data warehouse
    """
    secret_name = "database_credentials"
    secret = get_secret(secret_name)

    user = secret["user"]
    database = secret["database"]
    password = secret["password"]
    host = secret["host"]
    port = secret["port"]

    return Connection(
        user=user, database=database, password=password, host=host, port=port
    )

def create_s3_client():
    """
    Creates an S3 client using boto3
    """

    return boto3.client("s3")

def load_parquet(s3_client, bucket_name, key, table_name, conn):
    """ Loads a Parquet file from S3 into target table in the data warehouse

    s3_client: boto3 S3 client
    bucket_name: processed bucket
    key: key of the parquet file in the 3S bucket
    table_name: Table name in the DW
    conn: database connection
    """
    try:
        # Read Parquet file from S3
        # Fetches the file from S3 using the bucket name and key.
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        # Converts the binary content into an object that can be processed
        parquet_data = BytesIO(response["Body"].read())
        # Uses pandas to read the parquet data into a DataFrame.
        dataframe = pd.read_parquet(parquet_data)


        create_table(dataframe, table_name, conn)

        # Insert data into the table
        # Creates a cursor object to execute SQL commands on the PostgreSQL
        cursor = conn.curor()
        # Iterates over the rows of the DataFrame, returning the index(_)
        # row data(row) for each iteration.
        for _, row in dataframe.iterrows():
            # Retrieves the column names and join the column names into a comma-separated string(for SQL)
            columns = ",".join(dataframe.columns)
            #Creates a list of "%s" placeholders, one for each column.
            # and Joins the placeholders into a comma-separated string to parameterize SQL values.
            values = ",".join(["%s"] * len(dataframe.columns))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            # Executes the SQL INSERT statement.
            cursor.execute(sql, tuple(row))
        #Commits the transaction, saving the changes to the database.
        conn.commit()

        logging.info(f"Data loaded successfully")
        
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise

def create_table(dataframe, table_name, conn):

    """ Creates a table in the data warehouse if it doesn't exist """
    # Creates a cursor object to execute SQL commands on the PostgreSQL
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute(
        f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        );
        """
    )
    exists = cursor.fetchone()[0]
    if exists:
        logging.info(f"Table {table_name} already exists. Skipping creation.")
        return

    # Dynamically create the table schema based on the DataFrame













    