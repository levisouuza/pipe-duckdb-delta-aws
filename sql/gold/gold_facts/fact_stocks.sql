SELECT
    P.product_sk,
    S.store_sk,
    D.date_sk,
    SS.quantify
FROM stocks_snapshot SS
LEFT JOIN dim_products P ON SS.product_id = P.product_id
LEFT JOIN dim_stores S ON SS.store_id = S.store_id
LEFT JOIN dim_date D ON cast(strftime(SS.dt_stock, '%Y%m%d') as int) = D.date_id