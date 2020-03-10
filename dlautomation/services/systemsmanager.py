import boto3
import json

ssm_client = boto3.client("ssm")

class SystemsManager:

    @staticmethod
    def get_parameter_jr(name, decryption_status):
        response = ssm_client.get_parameter(
            Name=name,
            WithDecryption=decryption_status
        )
        json_response = json.loads(response["Parameter"]["Value"])
        return json_response
