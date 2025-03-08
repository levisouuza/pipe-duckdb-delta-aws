with dlt_order_items as (
select * from table_delta_df
),
arquivo_order_itens as
(
    select * from read_csv('{uri}/order_items.csv')
)

select ar.* from arquivo_order_itens ar
left join dlt_order_items dlt
on hash(ar.order_id, ar.item_id, ar.product_id) = hash(dlt.order_id, dlt.item_id, dlt.product_id)
where dlt.order_id is null;