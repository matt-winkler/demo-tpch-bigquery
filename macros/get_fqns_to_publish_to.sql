{% macro get_fqns_to_publish_to(published_name, destination_schemas) %}
  {% set result = [] %}

  {% for ds in destination_schemas %}

    {{ result.append(ds ~ '.' ~ published_name) }}
  
  {% endfor %}

  {{ return(result) }}
{% endmacro %}