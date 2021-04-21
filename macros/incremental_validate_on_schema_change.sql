{% macro incremental_validate_on_schema_change(on_schema_change, default_value='ignore') %}
   
   {% if on_schema_change not in ['sync', 'append', 'fail', 'ignore'] %}
     {{ return(default_value) }}

   {% else %}
     {{ return(on_schema_change) }}
   
   {% endif %}

{% endmacro %}


