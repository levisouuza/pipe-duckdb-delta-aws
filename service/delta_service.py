from deltalake import DeltaTable, write_deltalake
from model.config_variables import ConfigVariables
from model.parameter import Parameter


class DeltaService:
    def __init__(self, config: ConfigVariables):
        self._config = config
        self._aws_credentials = self._config.get_aws_credentials()

    def write_delta_buckets(
        self, bucket_name, dataframe, table_name, write_mode, schema_mode=None
    ):
        uri = f"s3://{bucket_name}/delta-operations/{table_name}"

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
        uri = f"s3://{bucket_name}/delta-operations/{table_name}"
        delta_table = DeltaTable(table_uri=uri, storage_options=self._aws_credentials)

        if return_to_df:
            return delta_table.to_pandas()

        return delta_table

    def write_data_incremental_delta(self, parameter: Parameter, dataframe_to_ingest):
        if parameter.incremental_data_df_validate and len(dataframe_to_ingest) > 0:
            self.write_delta_buckets(
                parameter.bucket_name,
                dataframe_to_ingest,
                parameter.table_name,
                "append",
            )

        if not parameter.incremental_data_df_validate:
            self.write_delta_buckets(
                parameter.bucket_name,
                dataframe_to_ingest,
                parameter.table_name,
                "append",
            )

