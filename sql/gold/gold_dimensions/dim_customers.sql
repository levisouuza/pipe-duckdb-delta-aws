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