from model.config_variables import ConfigVariables
from config.duckdb_config import DuckDbConfig
from service.delta_service import DeltaService
from utils.date_utils import get_yesterday_date
from constants.constants import TABLES


class GoldIngestionProcessor:
    def __init__(self, config: ConfigVariables):
        self._config = config
        self._duckdb = DuckDbConfig(self._config)
        self._duckdb.create_connection_duckdb()
        self._connection = self._duckdb.connection
        self._delta = DeltaService(self._config)

    def write_delta_gold_layer(self):
        orders_sales = self._delta.read_deltalake(
            self._config.buckets.silver, "orders_sales", True
        )

        ### dim products
        dim_products = self._connection.sql("""
            WITH products as (
            SELECT DISTINCT
                product_id,
                product_name,
                brand_name,
                category_name
                FROM orders_sales
            ),
            sk_products as 
            (
                SELECT
                    row_number() over (order by product_id) as product_sk,
                    product_id,
                    product_name,
                    brand_name,
                    category_name
                FROM products
            )
            
            SELECT * FROM sk_products
        """).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.gold,
            dim_products,
            "dim_products",
            "append"
        )

        ### dim customers
        dim_customers = self._connection.sql("""
                    WITH customers as (
                    SELECT DISTINCT
                        customers_id,
                        customers_name
                    FROM orders_sales
                    ),
                    sk_customers as 
                    (
                        SELECT
                            row_number() over (order by customers_id) as customer_sk,
                            customers_id,
                            customers_name
                        FROM customers
                    )

                    SELECT * FROM sk_customers
                """).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.gold,
            dim_customers,
            "dim_customers",
            "append"
        )

        ### dim staffs
        dim_staffs = self._connection.sql("""
                            WITH staffs as (
                            SELECT DISTINCT
                                staff_id,
                                staff_name
                            FROM orders_sales
                            ),
                            sk_staff as 
                            (
                                SELECT
                                    row_number() over (order by staff_id) as staff_sk,
                                    staff_id,
                                    staff_name
                                FROM staffs
                            )

                            SELECT * FROM sk_staff
                        """).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.gold,
            dim_staffs,
            "dim_staffs",
            "append"
        )

        ### dim stores
        dim_stores = self._connection.sql("""
                                    WITH stores as (
                                    SELECT DISTINCT
                                        store_id,
                                        store_name
                                    FROM orders_sales
                                    ),
                                    sk_store as 
                                    (
                                        SELECT
                                            row_number() over (order by store_id) as store_sk,
                                            store_id,
                                            store_name
                                        FROM stores
                                    )

                                    SELECT * FROM sk_store
                                """).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.gold,
            dim_stores,
            "dim_stores",
            "append"
        )

        ### dim tempo
        dim_date = self._connection.sql("""
            with tempo as
            (
            select 
                cast(strftime(generate_series, '%Y%m%d') as int) as date_id
                generate_series as date,
                ,year(generate_series) as year
                ,month(generate_series) as month
                ,day(generate_series) as day
            from generate_series(DATE '2010-01-01', DATE '2030-12-31', INTERVAL '1' DAY)
            ), sk_date as
            (
                select
                    row_number() over (order by date_id) as date_sk,
                    *
                from tempo
            )
            select * from sk_date
        """).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.gold,
            dim_date,
            "dim_date",
            "append"
        )

        ### fato sales

        dim_products_df = self._delta.read_deltalake(
            self._config.buckets.gold, "dim_products", True
        )

        dim_customers_df = self._delta.read_deltalake(
            self._config.buckets.gold, "dim_customers", True
        )

        dim_staffs_df = self._delta.read_deltalake(
            self._config.buckets.gold, "dim_staffs", True
        )

        dim_stores_df = self._delta.read_deltalake(
            self._config.buckets.gold, "dim_stores", True
        )

        dim_date_df = self._delta.read_deltalake(
            self._config.buckets.gold, "dim_date", True
        )

        fact_sales = self._connection.sql("""
            SELECT 
               P.product_sk
               ,C.customer_sk
               ,ST.staff_sk
               ,S.store_sk
               ,D.date_sk
               ,OS.item_id
               ,OS.order_id
               ,OS.order_date
               ,OS.quantity
               ,OS.list_price
               ,OS.discount
            FROM orders_sales OS
            LEFT JOIN dim_products P ON OS.product_id = P.product_id
            LEFT JOIN dim_customers C ON OS.customer_id = C.customer_id
            LEFT JOIN dim_staffs ST ON OS.staff_id  = ST.staff_id
            LEFT JOIN dim_stores S ON OS.store_id = S.store_id
            LEFT JOIN dim_date D ON cast(strftime(OS.order_date, '%Y%m%d') as int) = D.date_id
        """).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.gold,
            fact_sales,
            "fact_sales",
            "append"
        )

        ### fato stocks

        stocks_snapshot = self._delta.read_deltalake(
            self._config.buckets.silver, "stocks_snapshot", True
        )

        fact_stocks = self._connection.sql("""
            SELECT
                P.product_sk,
                S.store_sk,
                D.date_sk,
                SS.quantify
            FROM stocks_snapshot SS
            LEFT JOIN dim_products P ON SS.product_id = P.product_id
            LEFT JOIN dim_stores S ON SS.store_id = S.store_id
            LEFT JOIN dim_date D ON cast(strftime(SS.dt_stock, '%Y%m%d') as int) = D.date_id
        """).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.gold,
            fact_stocks,
            "fact_stocks",
            "append"
        )







