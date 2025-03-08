WITH customers as (
SELECT DISTINCT
    customer_id,
    customer_name
FROM orders_sales
),
sk_customers as
(
    SELECT
        row_number() over (order by customer_id) as customer_sk,
        customer_id,
        customer_name
    FROM customers
), dlt_gold_customers as
(
    SELECT distinct customer_id FROM delta_gold_dim
)

SELECT * FROM sk_customers
WHERE customer_id NOT IN (SELECT customer_id FROM dlt_gold_customers)