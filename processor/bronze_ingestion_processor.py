import json

from constants.constants import TABLES_BRONZE
from model.parameter import Parameter
from model.config_variables import ConfigVariables
from config.duckdb_config import DuckDbConfig
from service.delta_service import DeltaService
from service.s3_service import S3Service
from service.ssm_service import SsmService
from factory.increment_insert_load_factory import IncrementInsertLoadFactory

LAYER = "bronze"


class BronzeIngestionProcessor:
    def __init__(self, config: ConfigVariables):
        self._config = config
        self._ssm_service = SsmService(self._config)

        self._duckdb = DuckDbConfig(self._config)
        self._duckdb.create_connection_duckdb()
        self._connection = self._duckdb.connection

        self._s3_service = S3Service(self._config)

        self._delta = DeltaService(self._config)

    def write_delta_bronze_layer(self):
        for table in TABLES_BRONZE:
            print(f"Start processing bronze ingestion table: {table}")

            _parameter = Parameter.parse_obj(
                json.loads(self._ssm_service.get_parameter(LAYER, table))
            )

            uri = f"s3://{self._config.buckets.stage}/" f"delta-operations/{table}"

            print(f"uri stage path: {uri}")

            _parameter.uri_s3_table = uri

            dataframe = self._connection.sql(
                f"select * from read_csv('{_parameter.uri_s3_table}/{table}.csv')"
            ).to_df()

            if _parameter.first_load:
                print("First Load")
                self._delta.write_delta_buckets(
                    self._config.buckets.bronze, dataframe, table, "append"
                )

            else:
                print("Incremental Load")
                incremental_load = (
                    IncrementInsertLoadFactory.get_increment_insert_load_service(
                        _parameter,
                        self._config,
                        self._delta,
                        self._s3_service,
                        self._connection,
                    )
                )

                incremental_load.execute(dataframe=dataframe)

        self._connection.close()


_config = ConfigVariables()  # noqa
bronze_processor = BronzeIngestionProcessor(_config)
bronze_processor.write_delta_bronze_layer()
