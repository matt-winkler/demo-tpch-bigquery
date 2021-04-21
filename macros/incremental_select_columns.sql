{% materialization incremental_select_columns, adapter='bigquery' -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = config.get('should_full_refresh') -%}
  {%- set on_schema_change = config.get('on_schema_change') -%}
  {%- set source_relation_name = config.get('source_relation_name') -%}
  {%- set default_value = config.get('default_value') -%}

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

    -- check for new columns in tmp_relation vs. target_relation
    {% set source_columns = adapter.get_columns_in_relation(ref(source_relation_name)) %}
    {% set target_columns = adapter.get_columns_in_relation(target_relation) %}
     
    {% for col in source_columns %} 
      {% if col not in target_columns %}
       -- add the newly identified column to the target table. It will already exist in the temp relation because of CREATE TABLE AS from the source
       {% set build_sql = 'ALTER TABLE ' + target_relation.schema+'.'+target_relation.name + ' ADD COLUMN ' + col.name + ' INT64' %}
       {% do run_query(build_sql) %}
        
       -- this updates the values in the target relation based on passed in config. WHERE 1=1 assumes we want to update everything in the target BEFORE processing the incrementals
       {% set build_sql = 'UPDATE ' + target_relation.schema+'.'+target_relation.name + ' SET ' + col.name + '=' + default_value + ' WHERE 1=1' %}
       {% do run_query(build_sql) %}

      {% endif %}
    {% endfor %}

    -- TODO: Solution for automatically identifying the data type for use in column creation
    
    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}
           
    {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
    {% set dest_columns_arr = [] %}
    {% for col in dest_columns %}
      
      -- identify if columns have been removed from the source. If so, drop them from the target
      {% if col not in source_columns %}
        {% set build_sql = 'ALTER TABLE ' + target_relation.schema+'.'+target_relation.name + ' DROP COLUMN ' + col.name %}
        {% do run_query(build_sql) %}
      
      -- otherwise plan to update them in the target
      {% else %}
       {{ dest_columns_arr.append(col.name) }}
      
      {% endif %}

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