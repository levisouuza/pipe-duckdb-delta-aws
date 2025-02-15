from model.config_variables import ConfigVariables
from config.duckdb_config import DuckDbConfig
from service.delta_service import DeltaService


class GoldIngestionIncrementalProcessor:
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

        delta_gold_products = self._delta.read_deltalake(
            self._config.buckets.gold, "dim_products", True
        )

        ### dim products
        dim_products = self._connection.sql(
            """
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
            ), 
            dlt_gold_products as
            (
                SELECT distinct product_id FROM delta_gold_products
            )
            SELECT * FROM sk_products
            WHERE product_id NOT IN (SELECT product_id FROM dlt_gold_products)
            
        """
        ).to_df()

        if len(dim_products) > 0:
            self._delta.write_delta_buckets(
                self._config.buckets.gold, dim_products, "dim_products", "append"
            )

        ### dim customers

        delta_gold_customers = self._delta.read_deltalake(
            self._config.buckets.gold, "dim_customers", True
        )

        dim_customers = self._connection.sql(
            """
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
                    ), dlt_gold_customers as
                    (
                        SELECT distinct customers_id FROM delta_gold_customers
                    )
                    
                    SELECT * FROM sk_customers
                    WHERE customers_id NOT IN (SELECT customers_id FROM dlt_gold_customers)
                        
                """
        ).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.gold, dim_customers, "dim_customers", "append"
        )

        ### dim staffs

        delta_gold_staff = self._delta.read_deltalake(
            self._config.buckets.gold, "dim_staffs", True
        )
        dim_staffs = self._connection.sql(
            """
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
                            ), dlt_gold_staff as
                            (
                                SELECT distinct staff_id FROM delta_gold_staff
                            )
                            
                            SELECT * FROM sk_staff
                            WHERE staff_id NOT IN (SELECT customers_id FROM dlt_gold_staff)
                        """
        ).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.gold, dim_staffs, "dim_staffs", "append"
        )

        ### dim stores

        delta_gold_store = self._delta.read_deltalake(
            self._config.buckets.gold, "dim_stores", True
        )

        dim_stores = self._connection.sql(
            """
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
                                    ), dlt_gold_store as
                                    (
                                        SELECT distinct store_id FROM delta_gold_store
                                    )
                                    
                                    SELECT * FROM sk_store
                                    WHERE store_id NOT IN (SELECT store_id FROM dlt_gold_store)
                                """
        ).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.gold, dim_stores, "dim_stores", "append"
        )

        ### fato sales

        delta_gold_fact_sales = self._delta.read_deltalake(
            self._config.buckets.gold, "fact_sales", True
        )

        fact_sales = self._connection.sql(
            """
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
            WHERE OS.order_date > (SELECT MAX(order_date) FROM delta_gold_fact_sales)
        """
        ).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.gold, fact_sales, "fact_sales", "append"
        )

        ### fato stocks

        delta_gold_fact_stocks = self._delta.read_deltalake(
            self._config.buckets.gold, "fact_stocks", True
        )

        dim_date = self._delta.read_deltalake(
            self._config.buckets.gold, "dim_date", True
        )

        fact_stocks = self._connection.sql(
            """
            SELECT
                P.product_sk,
                S.store_sk,
                D.date_sk,
                SS.quantify
            FROM stocks_snapshot SS
            LEFT JOIN dim_products P ON SS.product_id = P.product_id
            LEFT JOIN dim_stores S ON SS.store_id = S.store_id
            LEFT JOIN dim_date D ON cast(strftime(SS.dt_stock, '%Y%m%d') as int) = D.date_id
            WHERE SS.dt_stock > (
            SELECT MAX(D.date)
            FROM delta_gold_fact_stocks FS
            LEFT JOIN dim_date D ON FS.date_sk = D.date_sk
            );
        """
        ).to_df()

        self._delta.write_delta_buckets(
            self._config.buckets.gold, fact_stocks, "fact_stocks", "append"
        )
