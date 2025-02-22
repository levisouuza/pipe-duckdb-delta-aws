from model.config_variables import ConfigVariables
from model.parameter import Parameter
from service.incremental_insert_load_service import IncrementalInsertLoadService
from service.delta_service import DeltaService
from service.s3_service import S3Service


class IncrementalInsertLoadSqlService(IncrementalInsertLoadService):
    def __init__(
            self, parameter: Parameter, config: ConfigVariables, delta_service: DeltaService, s3_service: S3Service, duck_connection
    ):
        super().__init__(parameter, config, delta_service, s3_service, duck_connection)

    def execute(self, dataframe = None, delta_table = None):
        print("reading delta lake table")
        table_delta_df = self.delta_service.read_deltalake(
            self.config.buckets.bronze, self.parameter.table_name, return_to_df=True
        )

        print(
            f"get query s3 path. "
            f"bucket:{self.parameter.bucket_name_script_sql_path} and "
            f"path: {self.parameter.sql_script_path}"
        )
        sql_query = self.s3_service.get_sql_file_from_s3(
            self.parameter.bucket_name_script_sql_path,
            self.parameter.sql_script_path
        )

        if self.parameter.replace_uri:
            sql_query = sql_query.replace('{uri}', self.parameter.uri_s3_table)

        print(f"Query to be executed: {sql_query}")

        increment_data_df = self.duck_connection.sql(sql_query).to_df()

        if len(increment_data_df) > 0:
            self.delta_service.write_delta_buckets(
                self.config.buckets.bronze, increment_data_df, self.parameter.table_name, "append"
            )
