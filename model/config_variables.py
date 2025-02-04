import json
from typing import Optional, List
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import os

load_dotenv()


class Buckets(BaseModel):
    stage: Optional[str]
    bronze: Optional[str]
    silver: Optional[str]
    gold: Optional[str]


class ConfigVariables(BaseSettings):
    aws_access_key_id: Optional[str]
    aws_secret_access_key: Optional[str]
    aws_region: Optional[str]
    buckets: Optional[Buckets]
    extension_duck: Optional[List]
    s3_endpoint: Optional[str]
    http_retries: Optional[int]
    http_retry_backoff: Optional[int]


    @classmethod
    def parse_buckets(cls, values):
        if isinstance(values.get("buckets"), str):
            try:
                return {**values, "buckets": Buckets(**json.loads(values["buckets"].replace("'", '"')))}
            except json.JSONDecodeError as error:
                raise ValueError(f"Erro ao decodificar JSON dos buckets: {error}")
        return values

    def get_aws_credentials(self):
        return {
            "AWS_ACCESS_KEY_ID": self.aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.aws_secret_access_key,
            "AWS_REGION": self.aws_region
        }