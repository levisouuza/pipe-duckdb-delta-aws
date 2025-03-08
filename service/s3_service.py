from botocore.client import ClientError

from model.config_variables import ConfigVariables
from service.aws_service import AwsService


class S3Service(AwsService):
    def __init__(self, config: ConfigVariables):

        self._client = S3Service.get_client(config, "s3")
        self._config = config

    def upload_file_s3(self, bucket: str, local_filename: str, s3_filename=None):
        name_object_s3 = s3_filename if s3_filename else local_filename
        try:
            self._client.upload_file(local_filename, bucket, name_object_s3)

        except ClientError as err:
            raise Exception(f"Error while upload file: {local_filename} -> {str(err)}")

    def get_sql_file_from_s3(self, bucket_name: str, file_key: str) -> str:
        try:
            response = self._client.get_object(Bucket=bucket_name, Key=file_key)
            sql_content = response["Body"].read().decode("utf-8")
            return sql_content
        except ClientError as err:
            raise Exception(f"Error search file: {file_key} -> {str(err)}")
