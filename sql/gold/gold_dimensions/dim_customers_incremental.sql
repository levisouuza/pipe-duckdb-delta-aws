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