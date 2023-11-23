# utils/file_utils.py

from configparser import ConfigParser
import logging
import pandas as pd
import boto3


logging.basicConfig(level=logging.INFO) 

def load_credentials(file_path='C:/Users/azgardan/Data_Migration_project/data_migration/config/config.ini'):
    config = ConfigParser()
    config.read(file_path)

    return {
        'oracle_username': config.get('database', 'oracle_username'),
        'oracle_password': config.get('database', 'oracle_password'),
        'oracle_host': config.get('database', 'oracle_host'),
        'oracle_port': config.get('database', 'oracle_port'),

        'postgres_username': config.get('database', 'postgres_username'),
        'postgres_password': config.get('database', 'postgres_password'),
        'postgres_host': config.get('database', 'postgres_host'),
        'postgres_port': config.get('database', 'postgres_port'),

        'ssh_host': config.get('ssh', 'ssh_host'),
        'ssh_port': config.get('ssh', 'ssh_port'),
        'ssh_username': config.get('ssh', 'ssh_username'),
        'ssh_private_key': config.get('ssh', 'ssh_private_key'),
        'ssh_passkey': config.get('ssh', 'ssh_passkey'),
        'local_port': config.get('ssh', 'local_port')
    }



def save_dataframe_to_csv(dataframe, file_path, index=False):
    try:
        dataframe.to_csv(file_path, index=index)
        print(f"DataFrame saved to {file_path}")
    except Exception as e:
        logging.error(f"Error saving DataFrame to CSV: {str(e)}")



def read_aws_credentials(config_file='C:/Users/azgardan/Data_Migration_project/data_migration/config/config.ini'):
    config = ConfigParser()
    config.read(config_file)

    aws_access_key_id = config.get('aws_credentials', 'aws_access_key_id')
    aws_secret_access_key = config.get('aws_credentials', 'aws_secret_access_key')
    aws_region = config.get('aws_credentials', 'aws_region')


    return aws_access_key_id, aws_secret_access_key, aws_region



def upload_to_s3(local_file_path, s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key):
    try:
        # Read data from local CSV file
        data = pd.read_csv(local_file_path)

        # Connect to S3
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        # Upload file to S3
        s3.upload_file(local_file_path, s3_bucket, s3_key)

        print("Data uploaded to S3 successfully!")

    except Exception as e:
        print(f"Error: {str(e)}")

    finally:
        # Cleanup or additional actions if needed
        pass


def download_csv_from_s3(bucket_name, file_key, local_path, aws_access_key_id, aws_secret_access_key, aws_region):
  
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)
    s3.download_file(bucket_name, file_key, local_path)




