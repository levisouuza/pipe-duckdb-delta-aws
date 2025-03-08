import json
from model.config_variables import ConfigVariables
from config.duckdb_config import DuckDbConfig
from model.parameter import Parameter
from service.delta_service import DeltaService
from service.s3_service import S3Service
from service.ssm_service import SsmService
from constants.constants import TABLES_SILVER


LAYER = "silver"


class SilverIngestionProcessor:
    def __init__(self, config: ConfigVariables):
        self._config = config
        self._ssm_service = SsmService(self._config)

        self._duckdb = DuckDbConfig(self._config)
        self._duckdb.create_connection_duckdb()
        self._connection = self._duckdb.connection

        self._s3_service = S3Service(self._config)

        self._delta = DeltaService(self._config)

    def write_delta_silver_layer(self):

        brands_delta = self._delta.read_deltalake(
            self._config.buckets.bronze, "brands", True
        )

        categories_delta = self._delta.read_deltalake(
            self._config.buckets.bronze, "categories", True
        )

        customers_delta = self._delta.read_deltalake(
            self._config.buckets.bronze, "customers", True
        )

        order_items_delta = self._delta.read_deltalake(
            self._config.buckets.bronze, "order_items", True
        )

        orders_delta = self._delta.read_deltalake(
            self._config.buckets.bronze, "orders", True
        )

        products_delta = self._delta.read_deltalake(
            self._config.buckets.bronze, "products", True
        )

        staffs_delta = self._delta.read_deltalake(
            self._config.buckets.bronze, "staffs", True
        )

        stocks_delta = self._delta.read_deltalake(
            self._config.buckets.bronze, "stocks", True
        )

        stores_delta = self._delta.read_deltalake(
            self._config.buckets.bronze, "stores", True
        )

        for table in TABLES_SILVER:

            print(f"Start processing silver ingestion table: {table}")

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
                    self._config.buckets.silver,
                    dataframe,
                    _parameter.table_name,
                    "append",
                )

            else:
                print("Incremental Load")
                sql_query = self._s3_service.get_sql_file_from_s3(
                    _parameter.bucket_name, _parameter.sql_script_path_incremental
                )

                orders_sales_silver = None
                if "orders_sales" == _parameter.table_name:
                    orders_sales_silver = self._delta.read_deltalake(
                        self._config.buckets.silver, "orders_sales", True
                    )

                dataframe = self._connection.sql(sql_query).to_df()

                self._write_data_incremental_delta(_parameter, dataframe)

    def _write_data_lake_first_load(self, parameter: Parameter):
        sql_query = self._s3_service.get_sql_file_from_s3(
            parameter.bucket_name, parameter.sql_script_path
        )

        dataframe = self._connection.sql(sql_query).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.silver,
            dataframe,
            parameter.table_name,
            "append",
        )

    def _write_data_lake_incremental_load(self, parameter: Parameter):
        sql_query = self._s3_service.get_sql_file_from_s3(
            parameter.bucket_name, parameter.sql_script_path_incremental
        )

        orders_sales_silver = None
        if "orders_sales" == parameter.table_name:
            orders_sales_silver = self._delta.read_deltalake(
                self._config.buckets.silver, "orders_sales", True
            )

        dataframe = self._connection.sql(sql_query).to_df()

        self._write_data_incremental_delta(parameter, dataframe)


    def _write_data_incremental_delta(self, parameter: Parameter, dataframe_to_ingest):
        if parameter.incremental_data_df_validate and len(dataframe_to_ingest) > 0:
            self._delta.write_delta_buckets(
                parameter.bucket_name,
                dataframe_to_ingest,
                parameter.table_name,
                "append",
            )

        if not parameter.incremental_data_df_validate:
            self._delta.write_delta_buckets(
                parameter.bucket_name,
                dataframe_to_ingest,
                parameter.table_name,
                "append",
            )


_config = ConfigVariables()  # noqa
silver_processor = SilverIngestionProcessor(_config)
silver_processor.write_delta_silver_layer()
