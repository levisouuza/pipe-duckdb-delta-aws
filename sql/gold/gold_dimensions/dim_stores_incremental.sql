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