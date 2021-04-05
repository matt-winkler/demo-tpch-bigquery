{{
    config(materialized='table')
}}

with orders as ( select * from {{ ref('order_items') }} ),

aggregated as (

    select order_key,
           SUM(gross_item_sales_amount) as gross_order_sales_amount
    from   orders 
    group by 1
)


select * from aggregated