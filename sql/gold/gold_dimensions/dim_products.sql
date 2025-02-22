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
