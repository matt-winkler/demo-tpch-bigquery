
with orders as (select * from {{ ref('stg_tpch_orders') }} )

select * from orders