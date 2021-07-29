{{
    config(
        materialized='table',
        post_hook=publish_model(
            this, 
            published_name=this.identifier ~ '__published', 
            destination_schemas=['test', 'test_again'],
            dry_run=False
          )
    )
}}


with source as (

    select * from {{ source('tpch', 'orders') }}

),

renamed as (

    select
    
        o_orderkey as order_key,
        o_custkey as customer_key,
        o_orderstatus as status_code,
        o_totalprice as total_price,
        o_orderdate as order_date,
        o_orderpriority as priority_code,
        o_clerk as clerk_name,
        o_shippriority as ship_priority,
        o_comment as comment

    from source

)

select * from renamed