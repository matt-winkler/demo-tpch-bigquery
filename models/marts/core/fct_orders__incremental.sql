{{ 
    config(
      materialized='dbt_incremental_select_columns',
      incremental_strategy='merge',
      unique_key='order_key',
      update_columns=['total_price', 'ship_priority', 'comment', 'order_key'],
      should_full_refresh=False
  )
}}

select
    *
from {{ ref('stg_tpch_orders') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- where order_date > (select max(order_date) from {{ this }} )
  where order_date not in (select order_date from {{ this }} group by 1)

{% endif %}


