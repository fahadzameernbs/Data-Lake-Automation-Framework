import boto3

#initialization
athena_client = boto3.client("athena")
s3_client = boto3.client("s3")

class Athena:

    @staticmethod
    def get_query_result(glue_db, query_string, s3_output_location_path, bucket_name):
        ex_id = None
        query_execution = athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={
                'Database': glue_db
            },
            ResultConfiguration={
                'OutputLocation': s3_output_location_path,
            }
        )
        while True:
            query_execution_response = athena_client.get_query_execution(
                QueryExecutionId=query_execution["QueryExecutionId"]
            )
            query_state = query_execution_response['QueryExecution']['Status']['State']
            if(query_state == "SUCCEEDED"):
                ex_id = query_execution["QueryExecutionId"]
                break
            elif(query_state == "FAILED"):
                print(query_execution_response['QueryExecution'])
                break
        query_file_path = "resultset/"+ex_id + ".csv"
        fileobj = s3_client.get_object(Bucket=bucket_name, Key=query_file_path)
        return fileobj
