# from pg8000.native import Connection
# import dotenv
# import os
# from datetime import datetime
# import logging
# import boto3
# from pprint import pprint
# import csv
# import io

# def connect():
#     dotenv.load_dotenv()

#     user = os.environ['user']
#     database = os.environ['database']
#     password = os.environ['password']
#     host = os.environ['host']
#     port = os.environ['port']

#     return Connection(

#         user=user,
#         database=database,
#         password=password,
#         host=host,
#         port=port
#     )

# def create_s3_client():
#     return boto3.client('s3')


# def create_file_name(query):
#     pass