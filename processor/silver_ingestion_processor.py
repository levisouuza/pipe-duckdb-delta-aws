from model.config_variables import ConfigVariables
from config.duckdb_config import DuckDbConfig
from service.delta_service import DeltaService
from utils.date_utils import get_yesterday_date
from constants.constants import TABLES


class SilverIngestionProcessor:
    def __init__(self, config: ConfigVariables):
        self._config = config
        self._duckdb = DuckDbConfig(self._config)
        self._duckdb.create_connection_duckdb()
        self._connection = self._duckdb.connection
        self._delta = DeltaService(self._config)

    def write_delta_silver_layer(self):

        brands_delta = self._delta.read_deltalake(
            self._config.buckets.bronze, "brands", True
        )

        categories_delta = self._delta.read_deltalake(
            self._config.buckets.bronze, "categories", True
        )

        customer_delta = self._delta.read_deltalake(
            self._config.buckets.bronze, "customer_delta", True
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

        orders_sales = self._connection.sql(""""
                 SELECT 
                    P.product_id
                    ,p.product_name
                    ,B.brand_name
                    ,CT.category_name
                    ,C.customer_id
                    ,C.first_name || C.last_name AS customer_name
                    ,S.staff_id
                    ,S.first_name || S.last_name AS staff_name
                    ,ST.store_id
                    ,ST.store_name
                    ,OI.order_id
                    ,OI.item_id
                    ,O.order_date
                    ,OI.quantity
                    ,OI.list_price
                    ,OI.discount
                FROM order_items_delta OI 
                LEFT JOIN orders_delta O ON OI.order_id = O.order_id
                LEFT JOIN products_delta P ON P.product_id = OI.product_id
                LEFT JOIN brands_delta B ON P.brand_id = B.brand_id
                LEFT JOIN categories_delta CT ON P.category_id = CT.category_id
                LEFT JOIN customers_delta C ON C.customer_id = O.customer_id
                LEFT JOIN staffs_delta S ON S.staff_id = O.staff_id 
                LEFT JOIN stores_delta ST ON ST.store_id = O.store_id
                ;
        """).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.silver,
            orders_sales,
            "orders_sales",
            "append"
        )


        # silver para base stocks

        stocks_snapshot = self._connection.sql("""
                    SELECT *, current_date as dt_stock from stocks_delta
        
        """).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.silver,
            stocks_snapshot,
            "stocks_snapshot",
            "append"
        )

        ### incremental para orders_sales

        orders_sales_bronze_delta = self._delta.read_deltalake(
            self._config.buckets.silver,
            "orders_sales", True
        )

        orders_sales = self._connection.sql(""""
                        WITH orders_sales_bronze as (
                         SELECT 
                            P.product_id
                            ,p.product_name
                            ,B.brand_name
                            ,CT.category_name
                            ,C.customer_id
                            ,C.first_name || C.last_name AS customer_name
                            ,S.staff_id
                            ,S.first_name || S.last_name AS staff_name
                            ,ST.store_id
                            ,ST.store_name
                            ,OI.order_id
                            ,OI.item_id
                            ,O.order_date
                            ,OI.quantity
                            ,OI.list_price
                            ,OI.discount
                        FROM order_items_delta OI 
                        LEFT JOIN orders_delta O ON OI.order_id = O.order_id
                        LEFT JOIN products_delta P ON P.product_id = OI.product_id
                        LEFT JOIN brands_delta B ON P.brand_id = B.brand_id
                        LEFT JOIN categories_delta CT ON P.category_id = CT.category_id
                        LEFT JOIN customers_delta C ON C.customer_id = O.customer_id
                        LEFT JOIN staffs_delta S ON S.staff_id = O.staff_id 
                        LEFT JOIN stores_delta ST ON ST.store_id = O.store_id
                        ), 
                        dt_orders_sales as 
                        (
                            select max(order_date) as order_date from orders_sales_bronze_delta
                        )
                        
                         select * from orders_sales_bronze
                         where order_sate > (select order_date from dt_orders_sales)          
                        ;
                """).to_df()

        if len(orders_sales) > 0:
            self._delta.write_delta_buckets(
                self._config.buckets.silver,
                orders_sales,
                "orders_sales",
                "append"
            )


