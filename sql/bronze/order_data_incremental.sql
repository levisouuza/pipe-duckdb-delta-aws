with order_baseline_csv as
(
   select * from read_csv('{uri}/orders.csv')
),
dlt_orders as (
    select max(order_date) as order_date from table_delta_df
)
SELECT ar.* FROM order_baseline_csv ar
where ar.order_date > (select order_date from dlt_orders)