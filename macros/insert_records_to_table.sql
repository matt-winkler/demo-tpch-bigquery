{% macro insert_records_to_table() %}
  {% set sql %}
    INSERT INTO 
      sales-demo-project-314714.dbt_mwinkler.ingestion_time_part_source (order_item_key)
    SELECT order_item_key
      FROM sales-demo-project-314714.dbt_mwinkler.fct_order_items
  {% endset %}

  {% do run_query(sql) %}

{% endmacro %}