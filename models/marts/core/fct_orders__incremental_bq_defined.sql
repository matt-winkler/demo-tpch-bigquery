{{ 
    config(
      materialized='incremental_custom',
      incremental_strategy='merge',
      unique_key='order_key',
      should_full_refresh=False,
      on_schema_change='append',
      set_defaults=True,
      default_values_map={
        'INT64': '-1',
        'STRING': 'emptystring'
        }
  )
}}

select
    *
from {{ ref('stg_tpch_orders') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where order_key not in (select order_key from {{ this }} group by 1)

{% endif %}


