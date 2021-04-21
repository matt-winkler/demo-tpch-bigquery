{% macro aqua_snapshot () %}
{% set models = ['dim_customers'] %}
{% for model in models %}
 {{clone('aqua.'+ model, 'dwh.aqua_'+model+'_m', 1 )}}
{% endfor %}
{% endmacro %}