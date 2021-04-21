{% macro clone( source_table, target_table,  backup=False) %}
  {% if source_table is not none %}
      {% set schema   = source_table.split('.')[0]  %}
      {% set table   = source_table.split('.')[1]  %}
			
            {% set test_transient1 %}
        
             show tables like '{{ target_table }}' in {{target.database}}.{{ schema }};
			
            {% endset %}
      	
          {% do  run_query(test_transient1) %}

   {% endif %}
{% endmacro %}