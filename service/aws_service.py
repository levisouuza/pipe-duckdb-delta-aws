import boto3
from model.config_variables import ConfigVariables


class AwsService:
    @classmethod
    def get_client(cls, config: ConfigVariables, service):
        return boto3.client(
            service,
            region_name="us-east-1",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        )