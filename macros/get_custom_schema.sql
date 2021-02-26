-- put this in macros/get_custom_schema.sql

{% macro generate_schema_name(custom_schema_name, node) -%}
    
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        --{{ default_schema }}_{{ custom_schema_name | trim }}

        {{ custom_schema_name }}

    {%- endif -%}

    {{ generate_schema_name_for_env(custom_schema_name, node) }}
{%- endmacro %}