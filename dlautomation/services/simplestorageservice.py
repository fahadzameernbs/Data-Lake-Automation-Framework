import boto3
from botocore.exceptions import ClientError

#initialization
s3_client = boto3.client("s3")

class SimpleStorageService:

    @staticmethod
    def create_Bucket(bucket_name):
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print("Create Bucket on S3 named {0}".format(bucket_name))
        except ClientError as ce:
            print(ce)

    @staticmethod
    def upload_file(file_path, bucket_name, file_name):
        try:
            s3_client.upload_file(file_path, bucket_name, file_name)
            print("upload '{0}' file into {1} S3 bucket".format(file_name, bucket_name))
        except ClientError as ce:
            print(ce)
