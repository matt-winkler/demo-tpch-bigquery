{% snapshot record_data_snapshot %}

{{
    config(
      target_database='fishtown-demo',
      target_schema='dbt_mwinkler',
      unique_key='a',
      strategy='check',
      check_cols=['aa.b', 'aa.c', 'xx.y', 'xx.z'],
    )
}}

select * from {{ source('tpch', 'record_type_data_example') }}

{% endsnapshot %}