
with record_data as (select * from {{ source('tpch', 'record_type_data_example') }} )

select * from record_data