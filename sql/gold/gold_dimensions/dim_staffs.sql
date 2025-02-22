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
)

SELECT * FROM sk_staff