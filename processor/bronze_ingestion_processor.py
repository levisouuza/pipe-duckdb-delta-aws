from model.config_variables import ConfigVariables
from config.duckdb_config import DuckDbConfig
from service.delta_service import DeltaService
from utils.date_utils import get_yesterday_date
from constants.constants import TABLES


class BronzeIngestionProcessor:
    def __init__(self, config: ConfigVariables):
        self._config = config

        self._duckdb = DuckDbConfig(self._config)
        self._duckdb.create_connection_duckdb()
        self._connection = self._duckdb.connection

        self._delta = DeltaService(self._config)


    def write_delta_bronze_layer(self):
        yesterday_date = get_yesterday_date()

        for table in TABLES:
            uri = (f"s3://{self._config.buckets.stage}/"
                   f"delta-operations/{yesterday_date}/"
                   f"{table}")

            dataframe = self._connection.sql(
                f"select * from read_csv('{uri}/{table}.csv')"
            ).to_df()

            self._delta.write_delta_buckets(
                self._config.buckets.bronze, dataframe, table, "append"
            )

            # carga incremental
            table_delta_response = self._delta.read_deltalake(
                self._config.buckets.bronze, table
            )

            column = ""
            if table[:-1] == "categorie":
                column = "category"
            else:
                column = table[:-1]

            (
                table_delta_response.merge(
                    source=dataframe,
                    predicate=f"target.{column}_id = source.{column}_id",
                    source_alias="source",
                    target_alias="target"
                ).when_not_matched_insert_all().execute()
            )

            # carga incremental para tabela order_items segundo o curso

            order_items = self._delta.read_deltalake(
                self._config.buckets.bronze, table
            )

            order_items_df = order_items.to_pandas()
            order_items_data = self._connection.sql(f"""
                with dlt_order_items as ( 
                select * from order_items
                ), 
                arquivo_order_itens as
                (
                    select * from read_csv('{uri}/{table}.csv')
                ) 
                
                select ar. from arquivo_order_itens ar
                left join dlt_order_items dlt 
                on hash(ar.order_id, ar.item_id, ar.produtct_id = hash(dlt.order_id, dlt.item_id, dlt.produtct_id
                where dlt.order_id is null;
    
            """).to_df()

            if len(order_items_data) > 0:
                self._delta.write_delta_buckets(
                    self._config.buckets.bronze, order_items_data, table, "append"
                )

            # carga incremental para tabela orders segundo o curso

            orders = self._delta.read_deltalake(
                self._config.buckets.bronze, table
            )

            orders_df = order_items.to_pandas()
            orders_data = self._connection.sql(f"""
                           with arquivo_order_itens as
                           (
                               select * from read_csv('{uri}/{table}.csv')
                           ),
                           dlt_orders as (
                                select max(order_date) as order_date from orders_df
                           )
                            SELECT ar.* FROM arquivo_orders ar
                            where ar.order_date > (select order_date from dlt_orders)

                       """).to_df()

            if len(orders_data) > 0:
                self._delta.write_delta_buckets(
                    self._config.buckets.bronze, orders_data, table, "append"
                )

        self._connection.close()


_config = ConfigVariables()  # noqa
stage_processor = BronzeIngestionProcessor(_config)
stage_processor.write_delta_bronze_layer()



