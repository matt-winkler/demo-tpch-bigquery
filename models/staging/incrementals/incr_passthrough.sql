
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by = {'field': 'part_time', 'data_type': 'timestamp'},
    )
}}

{% set distant_past = '1900-01-01' %}

{% if is_incremental() %}

{% set max_partition_time = get_max_partition('part_time', this ) %}

{% endif %}

with source_data as ( 
    select *, _PARTITIONTIME AS part_time
    from {{ 
        source('test', 'ingestion_time_part_source') 
    }} 
    {% if is_incremental() %}
      where date(_PARTITIONTIME) > '{{max_partition_time}}' --using {{ this }} trips error, even when using the ingestion time partition columns directly: Cannot query over table 'sales-demo-project-314714.dbt_mwinkler.ingestion_time_part_source' without a filter over column(s) '_PARTITION_LOAD_TIME', '_PARTITIONDATE', '_PARTITIONTIME' that can be used for partition elimination
    
    {% else %}
      where date(_PARTITIONTIME) >= date({{ distant_past }})
    
    {% endif %}

)

select * from source_data






