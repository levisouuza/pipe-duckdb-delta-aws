with tempo as
(
select
    cast(strftime(generate_series, '%Y%m%d') as int) as date_id
    generate_series as date,
    ,year(generate_series) as year
    ,month(generate_series) as month
    ,day(generate_series) as day
from generate_series(DATE '2010-01-01', DATE '2030-12-31', INTERVAL '1' DAY)
), sk_date as
(
    select
        row_number() over (order by date_id) as date_sk,
        *
    from tempo
)
select * from sk_date