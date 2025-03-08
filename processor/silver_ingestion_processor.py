import json

from constants.constants import TABLES_SILVER
from model.config_variables import ConfigVariables
from model.parameter import Parameter
from service.delta_service import DeltaService
from service.s3_service import S3Service
from service.ssm_service import SsmService

LAYER = "silver"


class SilverIngestionProcessor:
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

    def write_delta_silver_layer(self):

        brands_delta = self._delta.read_deltalake(  # noqa
            self._config.buckets.bronze, "brands", True
        )

        categories_delta = self._delta.read_deltalake(  # noqa
            self._config.buckets.bronze, "categories", True
        )

        customers_delta = self._delta.read_deltalake(  # noqa
            self._config.buckets.bronze, "customers", True
        )

        order_items_delta = self._delta.read_deltalake(  # noqa
            self._config.buckets.bronze, "order_items", True
        )

        orders_delta = self._delta.read_deltalake(  # noqa
            self._config.buckets.bronze, "orders", True
        )

        products_delta = self._delta.read_deltalake(  # noqa
            self._config.buckets.bronze, "products", True
        )

        staffs_delta = self._delta.read_deltalake(  # noqa
            self._config.buckets.bronze, "staffs", True
        )

        stocks_delta = self._delta.read_deltalake(  # noqa
            self._config.buckets.bronze, "stocks", True
        )

        stores_delta = self._delta.read_deltalake(  # noqa
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

                orders_sales_silver = None  # noqa
                if "orders_sales" == _parameter.table_name:
                    orders_sales_silver = self._delta.read_deltalake(  # noqa
                        self._config.buckets.silver, "orders_sales", True
                    )

                dataframe = self._connection.sql(sql_query).to_df()

                self._delta.write_data_incremental_delta(_parameter, dataframe)
