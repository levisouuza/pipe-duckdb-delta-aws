from model.config_variables import ConfigVariables
from service.aws_service import AwsService
from botocore.client import ClientError


class S3Service(AwsService):
    def __init__(self, config: ConfigVariables):

        self._client = S3Service.get_client(config, "s3")
        self._config = config

    def upload_file_s3(self, local_filename: str, s3_filename=None):
        name_object_s3 = s3_filename if s3_filename else local_filename
        try:
            self._client.upload_file(
                local_filename,
                self._config.buckets.stage,
                name_object_s3
            )

        except ClientError as err:
            raise Exception(f"Error while upload file: {local_filename} -> {str(err)}")

    def _get_object_in_bucket(self, bucket: str, key: str):
        resp = self._client.get_object(
            Bucket=bucket, Key=key
        )
        return resp.get("ResponseMetadata").get("HTTPStatusCode")