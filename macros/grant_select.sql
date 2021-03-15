{% macro grant_select(role) %}

{% set sql %}
    grant usage on schema {{ target.schema }} to role {{ role }};
    grant select on all tables in schema {{ target.schema }} to role {{ role }};
    grant select on all views in schema {{ target.schema }} to role {{ role }};

{% endset %}


{% do run_query(sql) %}

{% do log("Privileges granted", info=True) %}

{% endmacro %}