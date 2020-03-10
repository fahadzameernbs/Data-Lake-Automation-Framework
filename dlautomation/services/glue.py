import boto3
from botocore.exceptions import ClientError

#initialization
glue_client = boto3.client("glue")

class Glue:

    @staticmethod
    def create_database(db_name):
        try:
            glue_client.create_database(
                DatabaseInput={
                    'Name': db_name,
                }
            )
            print("Create database on Glue named {0}".format(db_name))
        except ClientError as ce:
            print(ce)

    @staticmethod
    def create_crawler(crawler_name, db_name, s3_bucket_path):
        try:
            glue_client.create_crawler(
                Name=crawler_name,
                DatabaseName=db_name,
                Role="arn:aws:iam::915054695365:role/awsroles-fahad-training",
                Targets={
                    'S3Targets': [
                        {
                            'Path': s3_bucket_path
                        }
                    ]
                }
            )
            print("Create crawler on Glue named '{0}'".format(crawler_name))
        except ClientError as ce:
            print(ce)

    @staticmethod
    def execute_crawler(crawler_name):
        try:
            glue_client.start_crawler(
                Name=crawler_name
            )
            print("Execution of '{0}' crawler has been started. Please wait to be COMPLETED...".format(crawler_name))
            #get crawler status
            while True:
                crawler_response = glue_client.get_crawler(
                    Name=crawler_name
                )
                crawler_state = crawler_response["Crawler"]["State"]
                if(crawler_state == "READY"):
                    print("Execution of '{0}' Crawler is completed".format(crawler_name))
                    break
        except ClientError as ce:
            print(ce)

    @staticmethod
    def get_table_metadata(db_name, table_name):
        metadata = glue_client.get_table(DatabaseName=db_name, Name=table_name)
        return metadata
