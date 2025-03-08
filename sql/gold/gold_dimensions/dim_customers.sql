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
)

SELECT * FROM sk_customers