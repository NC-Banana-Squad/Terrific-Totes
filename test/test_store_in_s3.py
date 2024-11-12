from src.extract import store_in_s3
from moto import mock_aws
from io import StringIO 
import boto3

@mock_aws
def test_store_in_s3():
    # Set up mock s3:
    bucket_name = 'test-bucket'
    file_name = 'test-file.csv'
    csv_content = 'column1,column2\nvalue1,value2'
    csv_buffer = StringIO(csv_content)

    # Initialise mock s3 client:
    mock_s3_client = boto3.client('s3', region_name='us-east-1')
    mock_s3_client.create_bucket(Bucket=bucket_name)

    # Call the function
    store_in_s3(mock_s3_client, csv_buffer, bucket_name, file_name)

    response = mock_s3_client.get_object(Bucket=bucket_name, Key=file_name)
    content = response['Body'].read().decode('utf-8')

    # Basic assertions
    assert content == csv_content