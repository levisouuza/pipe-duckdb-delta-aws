WITH staffs as (
SELECT DISTINCT
    staff_id,
    staff_name
FROM orders_sales
),
sk_staff as
(
    SELECT
        row_number() over (order by staff_id) as staff_sk,
        staff_id,
        staff_name
    FROM staffs
), dlt_gold_staff as
(
    SELECT distinct staff_id FROM delta_gold_staff
)

SELECT * FROM sk_staff
WHERE staff_id NOT IN (SELECT customers_id FROM dlt_gold_staff)