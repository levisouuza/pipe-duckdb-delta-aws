from model.config_variables import ConfigVariables
from model.parameter import Parameter
from service.incremental_insert_load_service import IncrementalInsertLoadService
from service.delta_service import DeltaService
from service.s3_service import S3Service


class IncrementalInsertLoadSqlService(IncrementalInsertLoadService):
    def __init__(
        self,
        parameter: Parameter,
        config: ConfigVariables,
        delta_service: DeltaService,
        s3_service: S3Service,
        duck_connection,
    ):
        super().__init__(parameter, config, delta_service, s3_service, duck_connection)

    def execute(self, dataframe=None, delta_table=None):
        table_delta_df = None

        if "bronze" == self.parameter.layer:
            print("reading delta lake table")
            table_delta_df = self.delta_service.read_deltalake(
                self.config.buckets.bronze, self.parameter.table_name, return_to_df=True
            )

        sql_script = (
            self.parameter.sql_script_path_incremental
            if self.parameter.sql_script_path_incremental
            else self.parameter.sql_script_path
        )

        print(
            f"get query s3 path. "
            f"bucket: {self.parameter.bucket_name} and "
            f"path: {sql_script}"
        )
        sql_query = self.s3_service.get_sql_file_from_s3(
            self.parameter.bucket_name, sql_script
        )

        if self.parameter.replace_uri:
            sql_query = sql_query.replace("{uri}", self.parameter.uri_s3_table)

        print(f"Query to be executed: {sql_query}")

        increment_data_df = self.duck_connection.sql(sql_query).to_df()

        self._write_data_delta(increment_data_df)

    def _write_data_delta(self, dataframe_to_ingest):
        if self.parameter.incremental_data_df_validate and len(dataframe_to_ingest) > 0:
            self.delta_service.write_delta_buckets(
                self.parameter.bucket_name,
                dataframe_to_ingest,
                self.parameter.table_name,
                "append",
            )

        if not self.parameter.incremental_data_df_validate:
            self.delta_service.write_delta_buckets(
                self.parameter.bucket_name,
                dataframe_to_ingest,
                self.parameter.table_name,
                "append",
            )
