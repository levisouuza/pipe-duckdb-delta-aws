SELECT
   P.product_sk
   ,C.customer_sk
   ,ST.staff_sk
   ,S.store_sk
   ,D.date_sk
   ,OS.item_id
   ,OS.order_id
   ,OS.order_date
   ,OS.quantity
   ,OS.list_price
   ,OS.discount
FROM orders_sales OS
LEFT JOIN dim_products P ON OS.product_id = P.product_id
LEFT JOIN dim_customers C ON OS.customer_id = C.customer_id
LEFT JOIN dim_staffs.sql ST ON OS.staff_id  = ST.staff_id
LEFT JOIN dim_stores S ON OS.store_id = S.store_id
LEFT JOIN dim_date D ON cast(strftime(OS.order_date, '%Y%m%d') as int) = D.date_id
WHERE OS.order_date > (SELECT MAX(order_date) FROM delta_gold_fact_sales)