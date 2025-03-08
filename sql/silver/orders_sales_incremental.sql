WITH orders_sales_bronze AS (
    SELECT
        P.product_id,
        P.product_name,
        B.brand_name,
        CT.category_name,
        C.customer_id,
        C.first_name || ' ' || C.last_name AS customer_name,
        S.staff_id,
        S.first_name || ' ' || S.last_name AS staff_name,
        ST.store_id,
        ST.store_name,
        OI.order_id,
        OI.item_id,
        O.order_date,
        OI.quantity,
        OI.list_price,
        OI.discount
    FROM order_items_delta OI
    LEFT JOIN orders_delta O ON OI.order_id = O.order_id
    LEFT JOIN products_delta P ON P.product_id = OI.product_id
    LEFT JOIN brands_delta B ON P.brand_id = B.brand_id
    LEFT JOIN categories_delta CT ON P.category_id = CT.category_id
    LEFT JOIN customers_delta C ON C.customer_id = O.customer_id
    LEFT JOIN staffs_delta S ON S.staff_id = O.staff_id
    LEFT JOIN stores_delta ST ON ST.store_id = O.store_id
),
dt_orders_sales AS (
    SELECT MAX(order_date) AS order_date FROM orders_sales_silver
)

SELECT *
FROM orders_sales_bronze
WHERE order_date > (SELECT order_date FROM dt_orders_sales);
