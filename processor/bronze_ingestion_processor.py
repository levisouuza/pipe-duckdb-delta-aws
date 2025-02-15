import json

from utils.date_utils import get_yesterday_date

from constants.constants import TABLES
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
        yesterday_date = get_yesterday_date()

        for table in TABLES[:2]:

            _parameter = Parameter.parse_obj(
                json.loads(self._ssm_service.get_parameter(LAYER, table))
            )

            uri = (
                f"s3://{self._config.buckets.stage}/"
                f"delta-operations/{yesterday_date}/"
                f"{table}"
            )

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
                        _parameter, self._config, self._delta, self._s3_service, self._connection
                    )
                )

                incremental_load.execute(dataframe=dataframe)

        self._connection.close()

            #
            # # carga incremental para tabela order_items segundo o curso
            #
            # order_items = self._delta.read_deltalake(
            #     self._config.buckets.bronze, table, True
            # )
            #
            # order_items_data = self._connection.sql(
            #     f"""
            #     with dlt_order_items as (
            #     select * from order_items
            #     ),
            #     arquivo_order_itens as
            #     (
            #         select * from read_csv('{uri}/{table}.csv')
            #     )
            #
            #     select ar. from arquivo_order_itens ar
            #     left join dlt_order_items dlt
            #     on hash(ar.order_id, ar.item_id, ar.produtct_id = hash(dlt.order_id, dlt.item_id, dlt.produtct_id
            #     where dlt.order_id is null;
            #
            # """
            # ).to_df()
            #
            # if len(order_items_data) > 0:
            #     self._delta.write_delta_buckets(
            #         self._config.buckets.bronze, order_items_data, table, "append"
            #     )
            #
            # # carga incremental para tabela orders segundo o curso
            #
            # orders = self._delta.read_deltalake(
            #     self._config.buckets.bronze, table, True
            # )
            #
            # orders_data = self._connection.sql(
            #     f"""
            #                with arquivo_order_itens as
            #                (
            #                    select * from read_csv('{uri}/{table}.csv')
            #                ),
            #                dlt_orders as (
            #                     select max(order_date) as order_date from orders
            #                )
            #                 SELECT ar.* FROM arquivo_orders ar
            #                 where ar.order_date > (select order_date from dlt_orders)
            #
            #            """
            # ).to_df()
            #
            # if len(orders_data) > 0:
            #     self._delta.write_delta_buckets(
            #         self._config.buckets.bronze, orders_data, table, "append"
            #     )


_config = ConfigVariables()  # noqa
stage_processor = BronzeIngestionProcessor(_config)
stage_processor.write_delta_bronze_layer()
