{{ 
    config(
      materialized='incremental_select_columns',
      incremental_strategy='merge',
      unique_key='order_key',
      should_full_refresh=False,
      on_schema_change='fail',
      source_relation_name='stg_tpch_orders',
      default_value='-1'
  )
}}


select
    *
from {{ ref('stg_tpch_orders') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where order_key not in (select order_key from {{ this }} group by 1)

{% endif %}


