{% macro dbt_get_incremental_sql(tmp_relation, target_relation, unique_key, dest_columns) %}

merge into {{ target_relation }} as DBT_INTERNAL_DEST

using (
    select *
    from {{ tmp_relation }}
    -- this filter will only be applied on an incremental run
    where {{ unique_key }} > (select max( {{ unique_key }} ) from {{ target_relation }} )

) as DBT_INTERNAL_SOURCE

on FALSE
    
when not matched then insert
    ( {{ dest_columns|join(', ') }} )
values
    ( {{ dest_columns|join(', ') }} )

{% endmacro %}