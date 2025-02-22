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