import pandas as pd
import boto3
import io

def counterparty(df=None):
    s3_client = boto3.client("s3", region_name='eu-west-2')
    obj_counterparty = s3_client.get_object(Bucket='banana-squad-ingested-data', Key='counterparty/2024/11/18/2024-11-18T15:24:20.781745.csv')
    obj_address = s3_client.get_object(Bucket='banana-squad-ingested-data', Key='address/2024/11/18/2024-11-18T15:24:22.587858.csv')

    #reads the botocore file stream
    file_stream_counterparty = io.StringIO(obj_counterparty['Body'].read().decode('utf-8'))
    file_stream_address = io.StringIO(obj_address['Body'].read().decode('utf-8'))
    #reads csv and makes a df. Loads data into df
    df_counterparty = pd.read_csv(file_stream_counterparty)
    df_address = pd.read_csv(file_stream_address)  
    print(df_counterparty)
    print(df_address)

counterparty()