from model.config_variables import ConfigVariables
from model.parameter import Parameter
from service.delta_service import DeltaService
from service.incremental_insert_load_service import IncrementalInsertLoadService
from service.s3_service import S3Service


class IncrementalInsertLoadMergeService(IncrementalInsertLoadService):
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

        table_delta_response = self.delta_service.read_deltalake(
            self.config.buckets.bronze, self.parameter.table_name
        )

        self._execute_merge(table_delta_response, dataframe)

    def _execute_merge(self, table_delta, dataframe):

        column = self._build_column_key_value_predicate()

        (
            table_delta.merge(
                source=dataframe,
                predicate=f"target.{column}_id = source.{column}_id",
                source_alias="source",
                target_alias="target",
            )
            .when_not_matched_insert_all()
            .execute()
        )

    def _build_column_key_value_predicate(self):
        if self.parameter.table_name[:-1] == "categorie":
            column = "category"
        else:
            column = self.parameter.table_name[:-1]

        return column
