{% macro set_default_values(new_columns, target_relation, default_values_map) %}
   
     {% for col in new_columns %}
     
       {% if col.dtype == 'STRING' %}
         {% set build_sql = 'UPDATE ' + target_relation.schema+'.'+target_relation.name + ' SET ' + col.name + '="' + default_values_map[col.dtype] + '"' + ' WHERE 1=1' %}
           {% do run_query(build_sql) %}
       
       {% else %}
         {% set build_sql = 'UPDATE ' + target_relation.schema+'.'+target_relation.name + ' SET ' + col.name + '=' + default_values_map[col.dtype] + ' WHERE 1=1' %}
           {% do run_query(build_sql) %}
        
        {% endif %}
    
    {% endfor %}

{% endmacro %}