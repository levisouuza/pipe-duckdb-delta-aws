WITH tempo AS (
    SELECT 
        CAST(strftime('%Y%m%d', generate_series) AS INT) AS date_id,
        generate_series AS date,
        year(generate_series) AS year,
        month(generate_series) AS month,
        day(generate_series) AS day
    FROM generate_series(DATE '2010-01-01', DATE '2030-12-31', INTERVAL 1 DAY)
), 
sk_date AS (
    SELECT 
        row_number() OVER (ORDER BY date_id) AS date_sk,
        *
    FROM tempo
)
SELECT * FROM sk_date;