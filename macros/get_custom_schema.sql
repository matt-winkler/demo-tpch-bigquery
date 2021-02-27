{% macro generate_schema_name(custom_schema_name, node) -%}
    
    {% set default_schema = target.schema %}
    {%- if target.name != 'dev' and custom_schema_name is not none -%}
       
        {{ custom_schema_name }}

    {%- else -%}
        
        {{ default_schema }}_{{ custom_schema_name }}
    
    {%- endif -%}

{%- endmacro %}