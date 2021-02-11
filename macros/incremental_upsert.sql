{% macro incremental_upsert(tmp_relation, target_relation, unique_key=none, dest_columns=none, statement_name="main") %}
    
    -- If we aren't given a set of destination columns to use, target all of them
    {%- if dest_columns is none -%}
        {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- endif -%}
    
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    {%- if unique_key is not none -%}
    delete
    from {{ target_relation }}
    where ({{ unique_key }}) in (
        select ({{ unique_key }})
        from {{ tmp_relation }}
    );
    {%- endif %}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation }}
    );
{%- endmacro %}
