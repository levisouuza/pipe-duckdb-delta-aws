from deltalake import DeltaTable, write_deltalake
from model.config_variables import ConfigVariables
from utils.date_utils import get_yesterday_date


class DeltaService:
    def __init__(self, config: ConfigVariables):
        self._config = config
        self._aws_credentials = self._config.get_aws_credentials()
        self._yesterday_date = get_yesterday_date()

    def write_delta_buckets(
        self, bucket_name, dataframe, table_name, write_mode, schema_mode=None
    ):
        uri = (
            f"s3://{bucket_name}/delta-operations/"
            f"{self._yesterday_date}/{table_name}"
        )

        if schema_mode:
            write_deltalake(
                table_or_uri=uri,
                data=dataframe,
                schema_mode=schema_mode,
                mode=write_mode,
                storage_options=self._aws_credentials,
            )
        else:
            write_deltalake(
                table_or_uri=uri,
                data=dataframe,
                mode=write_mode,
                storage_options=self._aws_credentials,
            )

    def read_deltalake(self, bucket_name, table_name, return_to_df: bool = False):
        uri = (
            f"s3://{bucket_name}/delta-operations/"
            f"{self._yesterday_date}/{table_name}"
        )
        delta_table = DeltaTable(table_uri=uri, storage_options=self._aws_credentials)

        if return_to_df:
            return delta_table.to_pandas()

        return delta_table

    @staticmethod
    def insert_when_not_match_keys(delta_table, dataframe, column_key):
        (
            delta_table.merge(
                source=dataframe,
                predicate=f"target.{column_key}_id = source.{column_key}_id",
                source_alias="source",
                target_alias="target",
            )
            .when_not_matched_insert_all()
            .execute()
        )
