from model.config_variables import ConfigVariables
from service.aws_service import AwsService
from botocore.client import ClientError


class SsmService(AwsService):
    def __init__(self, config: ConfigVariables):
        self._client = SsmService.get_client(config, "ssm")

    def get_parameter(self, layer: str, table_name: str):
        try:
            response = self._client.get_parameter(
                Name=self._build_path_params_name(layer, table_name)
            )
            return response.get("Parameter").get("Value")
        except ClientError as err:
            raise Exception(f"Parameter not Found: {str(err)}")

    def put_parameter(self, layer: str, table_name: str, params_value_to_test: str):
        try:
            self._client.put_parameter(
                Name=f"{self._build_path_params_name(layer, table_name)}",
                Description="string",
                Value=params_value_to_test,
                Overwrite=True,
                Type="String",
                Tier="Standard",
            )
            return 200

        except ClientError as err:
            raise ValueError(f"Put Parameter test Failed: {str(err)}")

    @staticmethod
    def _build_path_params_name(layer, table_name):
        return f"/{layer}/delta-operations/{table_name}"
