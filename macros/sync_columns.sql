{% macro sync_columns(on_schema_change, source_relation, target_relation) %}
  {# if on_schema_change is append or sync, we perform the according action. Otherwise this is a noop #}
  
  {%- set source_columns = adapter.get_columns_in_relation(source_relation) -%}
  {%- set target_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set remove_from_target_arr = [] %}
  {%- set add_to_target_arr = [] %}

  -- identify what's new
  {%- for col in source_columns -%} 
    {%- if col not in target_columns -%}
      {{ add_to_target_arr.append(col) }}
    {%- endif -%}
  {%- endfor -%}

  -- identify what's no longer relevant
  {%- for col in target_columns -%}
    {%- if col not in source_columns -%}
      {{ remove_from_target_arr.append(col) }}
    {%- endif -%}
  {%- endfor -%}

  -- if we are append only, make that change
  {%- if on_schema_change == 'append' -%}
    {%- for col in add_to_target_arr -%}
       {%- set build_sql = 'ALTER TABLE ' + target_relation.schema+'.'+target_relation.name + ' ADD COLUMN ' + col.name + ' ' + col.dtype -%}
       {%- do run_query(build_sql) -%}
    {%- endfor -%}
  
  -- if we are syncing, do that instead
  {%- elif on_schema_change == 'sync' %}
    
    -- first remove what we don't need
    {%- for col in remove_from_target_arr -%}
      {%- set build_sql = 'ALTER TABLE ' + target_relation.schema+'.'+target_relation.name + ' DROP COLUMN ' + col.name -%}
      {%- do run_query(build_sql) -%}
    {%- endfor -%}
    -- now add what we do
    {%- for col in add_to_target_arr -%}
       {%- set build_sql = 'ALTER TABLE ' + target_relation.schema+'.'+target_relation.name + ' ADD COLUMN ' + col.name + ' ' + col.dtype -%}
       {%- do run_query(build_sql) -%}
    {%- endfor -%}

  {%- endif -%}
  
  -- check whether the schema changed
  {% if add_to_target_arr != [] or remove_from_target_arr != [] %}
    {%- set schema_changed = True -%}
  {% else %}
    {%- set schema_changed = False -%}
  {% endif %}
  -- return the list of columns added so we can set defaults if we have them
  {{ 
      return(
             {
              'schema_changed': schema_changed,
              'new_columns': add_to_target_arr
             }
          )
  }}
  
{% endmacro %}
