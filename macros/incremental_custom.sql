{% materialization incremental_custom, adapter='bigquery' -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = config.get('should_full_refresh') -%}
  {%- set set_defaults = config.get('set_defaults') -%}
  {%- set default_values_map = config.get('default_values_map') -%}


  -- if the on_schema_change parameter isn't specified, set it to ignore to maintain current behavior
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change')) -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set strategy = dbt_bigquery_validate_get_incremental_strategy(config) -%}
  --{% set strategy = config.get('incremental_strategy') -%}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  
  {% elif existing_relation.is_view %}
    {# --Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  
  {% elif full_refresh_mode %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  
  {% else %}
  
    -- update the temp relation first
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}

    -- sync the schemas on the temp and target relations according to the config.
    {% set schema_changes = sync_columns(on_schema_change, tmp_relation, target_relation) %}
    {% set schema_changed = schema_changes['schema_changed'] %}
    {% set new_columns = schema_changes['new_columns'] %}

    -- if the schema changed and we want to fail, do that
    {% if schema_changed and on_schema_change == 'fail' %}
      {{ 
          exceptions.raise_compiler_error('The source and target schemas on this incremental model are out of sync!
               Please re-run the incremental model with full_refresh set to True to update the target schema.
               Alternatively, you can update the schema manually and re-run the process.') 
      }}
    {% endif %}

    -- if set_defaults is True and there are new_columns, set the default values passed in based on column types
    -- TODO: do we need to check if a default has already been set on this, or is this enough?
    {% if set_defaults and new_columns != [] %}
       {{ set_default_values(new_columns, target_relation, default_values_map) }}
    {% endif %}
     
    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}
           
    {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
    {% set dest_columns_arr = [] %}
    {% for col in dest_columns %}
      {{ dest_columns_arr.append(col.name) }}
    {% endfor %}
    
    {% set build_sql = dbt_get_incremental_sql(tmp_relation, target_relation, unique_key, dest_columns_arr) %}
  
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% set target_relation = target_relation.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}
  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}