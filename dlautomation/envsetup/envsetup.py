from services.simplestorageservice import SimpleStorageService
from services.glue import Glue
from resources.config import get_param

#----fetch configuration parameters
params = get_param("../resources/configurations.ini","CONFIGURATIONS")

#create bucket on s3
SimpleStorageService.create_Bucket(params["s3_bucket_name"])

#upload users file in s3 bucket
SimpleStorageService.upload_file(params["user_csv_file_path"], params["s3_bucket_name"], params["s3_user_file_name"])

#upload subjects file in s3 bucket
SimpleStorageService.upload_file(params["subject_csv_file_path"], params["s3_bucket_name"], params["s3_subject_file_name"])

#create database on glue
Glue.create_database(params["glue_db_name"])

#create crawler on glue to fetch the users meta data
Glue.create_crawler(params["glue_user_crawler_name"], params["glue_db_name"], params["s3_user_crawler_path"])

#create crawler on glue to fetch the subjects meta data
Glue.create_crawler(params["glue_subject_crawler_name"], params["glue_db_name"], params["s3_subject_crawler_path"])

#execute crawler to fetch the users meta data
Glue.execute_crawler(params["glue_user_crawler_name"])

#execute crawler to fetch the subjects meta data
Glue.execute_crawler(params["glue_subject_crawler_name"])
