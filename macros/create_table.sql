{% macro create_ingestion_time_part_table() %}
  {% set sql %}
    CREATE OR REPLACE TABLE
      sales-demo-project-314714.dbt_mwinkler.ingestion_time_part_source (order_item_key STRING)
    PARTITION BY
      DATE(_PARTITIONTIME)
    OPTIONS(
      partition_expiration_days=3,
      require_partition_filter=true
    )
  {% endset %}

  {% do run_query(sql) %}

{% endmacro %}