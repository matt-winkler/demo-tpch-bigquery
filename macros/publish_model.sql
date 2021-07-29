{% macro publish_model(model=None, published_name=None, destination_schemas=[], dry_run=True) %}
  {% set destination_fqns = get_fqns_to_publish_to(published_name, destination_schemas) %}
  {% do log('destination_fqns ' ~ destination_fqns, True) %}
  {% set model_fqn = this.project ~ '.' ~ this.dataset ~ '.' ~ this.identifier %}
  {% do log('Publishing model ' ~ model_fqn, True) %}
  {% for destination_fqn in destination_fqns %}
    {% set publish_ddl = 'create or replace table ' ~ destination_fqn ~ ' as select * from ' ~ model_fqn %}
    {% if execute and not dry_run %}
      {% do log('Publishing to ' ~ destination_fqn, True) %}
      {% do run_query(publish_ddl) %}
    {% else %}
      {% do log(publish_ddl, True) %}
    {% endif %}
  {% endfor %}
{% endmacro %}