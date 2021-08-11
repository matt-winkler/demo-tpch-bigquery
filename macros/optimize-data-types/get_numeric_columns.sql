{% macro get_numeric_columns_in_dataset(project_id, dataset) %}
  {% set get_columns_query %}
    SELECT 
      column_name, data_type
    FROM
      {{project_id}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS
    WHERE
      (data_type = 'BOOLEAN' OR 
       data_type = 'FLOAT' OR
       data_type = 'INTEGER' OR
      data_type = 'NUMERIC'  )
  {% endset %}

  {% if execute %}
    {% set results = run_query(get_columns_query) %}
    {% do log(results, info=true) %}
  {% endif %}

{% endmacro %}