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
    SELECT distinct product_id FROM delta_gold_dim
)
SELECT * FROM sk_products
WHERE product_id NOT IN (SELECT product_id FROM dlt_gold_products)