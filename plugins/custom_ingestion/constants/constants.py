LOCAL_FOLDER_DATASETS = "../datasets/"

TABLES_BRONZE = [
    "brands",
    "categories",
    "customers",
    "products",
    "staffs",
    "stocks",
    "stores",
    "order_items",
    "orders",
]

TABLES_SILVER = ["stocks_snapshot", "orders_sales"]

TABLES_GOLD_DIMENSIONS = [
    "dim_products",
    "dim_customers",
    "dim_staffs",
    "dim_stores",
    "dim_date",
]

TABLES_GOLD_FACTS = ["fact_sales", "fact_stocks"]
