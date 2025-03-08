import json
from model.config_variables import ConfigVariables
from config.duckdb_config import DuckDbConfig
from model.parameter import Parameter
from service.delta_service import DeltaService
from service.s3_service import S3Service
from service.ssm_service import SsmService
from constants.constants import TABLES_GOLD_DIMENSIONS


LAYER = "gold"


class GoldDimensionsIngestionProcessor:
    def __init__(self, config: ConfigVariables):
        self._config = config
        self._ssm_service = SsmService(self._config)

        self._duckdb = DuckDbConfig(self._config)
        self._duckdb.create_connection_duckdb()
        self._connection = self._duckdb.connection

        self._s3_service = S3Service(self._config)

        self._delta = DeltaService(self._config)

    def write_delta_gold_layer(self):

        print(
            f"reading delta lake table orders_sales "
            f"in bucket {self._config.buckets.silver}"
        )

        orders_sales = self._delta.read_deltalake(
            self._config.buckets.silver, "orders_sales", True
        )

        for table in TABLES_GOLD_DIMENSIONS:

            print(f"Start processing gold ingestion table: {table}")

            _parameter = Parameter.parse_obj(
                json.loads(self._ssm_service.get_parameter(LAYER, table))
            )

            if _parameter.first_load:
                print("First Load")

                sql_query = self._s3_service.get_sql_file_from_s3(
                    _parameter.bucket_name, _parameter.sql_script_path
                )

                dataframe = self._connection.sql(sql_query).to_df()

                self._delta.write_delta_buckets(
                    self._config.buckets.gold,
                    dataframe,
                    _parameter.table_name,
                    "append",
                )

            if not _parameter.first_load and _parameter.table_name not in "dim_date":
                print("Incremental Load")

                delta_gold_dim = self._delta.read_deltalake(
                    self._config.buckets.gold, _parameter.table_name, True
                )

                sql_query = self._s3_service.get_sql_file_from_s3(
                    _parameter.bucket_name, _parameter.sql_script_path_incremental
                )

                dataframe = self._connection.sql(sql_query).to_df()

                self._delta.write_data_incremental_delta(_parameter, dataframe)


_config = ConfigVariables()  # noqa
gold_dimensions_processor = GoldDimensionsIngestionProcessor(_config)
gold_dimensions_processor.write_delta_gold_layer()