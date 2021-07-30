{% macro get_max_partition(partition_column, this_table) %}
  
  {% set max_partitiontime_sql %}
    select date(max({{ partition_column }})) from {{ this_table }}
  {% endset %}

  {% set results = run_query(max_partitiontime_sql) %}
      
  {% if execute %}
    {# Return the first column #}
    {% set max_partition_time = results.columns[0].values()[0] %}
  {% endif %}

  {{return(max_partition_time)}}
  
{% endmacro %}