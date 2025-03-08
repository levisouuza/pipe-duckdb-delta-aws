import json

from constants.constants import TABLES_BRONZE
from factory.increment_insert_load_factory import IncrementInsertLoadFactory
from model.config_variables import ConfigVariables
from model.parameter import Parameter
from service.delta_service import DeltaService
from service.s3_service import S3Service
from service.ssm_service import SsmService

LAYER = "bronze"


class BronzeIngestionProcessor:
    def __init__(
        self,
        config: ConfigVariables,
        ssm_service: SsmService,
        connection,
        s3_service: S3Service,
        delta_service: DeltaService,
    ):
        self._config = config
        self._ssm_service = ssm_service
        self._connection = connection
        self._s3_service = s3_service
        self._delta = delta_service

    def write_delta_bronze_layer(self):
        for table in TABLES_BRONZE:
            print(f"Start processing bronze ingestion table: {table}")

            _parameter = Parameter.parse_obj(
                json.loads(self._ssm_service.get_parameter(LAYER, table))
            )

            uri = f"s3://{self._config.buckets.stage}/delta-operations/{table}"

            print(f"uri stage path: {uri}")

            _parameter.uri_s3_table = uri

            dataframe = self._connection.sql(
                f"select * from " f"read_csv('{_parameter.uri_s3_table}/{table}.csv')"
            ).to_df()

            if _parameter.first_load:
                print("First Load")
                self._delta.write_delta_buckets(
                    self._config.buckets.bronze, dataframe, table, "append"
                )

            else:
                print("Incremental Load")
                incremental_load = IncrementInsertLoadFactory.get_increment_insert_load_service(  # noqa
                    _parameter,
                    self._config,
                    self._delta,
                    self._s3_service,
                    self._connection,
                )

                incremental_load.execute(dataframe=dataframe)

        self._connection.close()
