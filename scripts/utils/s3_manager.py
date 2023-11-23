# s3_manager.py
import boto3
from botocore.exceptions import NoCredentialsError

class S3Manager:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.client("s3")

    def upload_file(self, key, local_file):
        try:
            self.s3.upload_file(local_file, self.bucket_name, key)
            print(f"File {key} uploaded successfully.")
        except NoCredentialsError:
            print("Credentials not available or not valid.")

    def download_file(self, key, local_file):
        try:
            self.s3.download_file(self.bucket_name, key, local_file)
            print(f"File {key} downloaded successfully.")
        except NoCredentialsError:
            print("Credentials not available or not valid.")
    

    def delete_file(self, key):
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=key)
            print(f"File {key} deleted successfully.")
        except NoCredentialsError:
            print("Credentials not available or not valid.")
