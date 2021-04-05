{{
    config(materialized='table')
}}

{% call set_sql_header(config) %}

CREATE TEMPORARY FUNCTION yes_no_to_boolean(answer STRING)
RETURNS BOOLEAN AS (
  CASE
  WHEN LOWER(answer) = 'yes' THEN True
  WHEN LOWER(answer) = 'no' THEN False
  ELSE NULL
  END
);

{% endcall %}

with bool_test as (

    select 1 as Id, 'yes' as Answer UNION ALL
    select 1 as Id, 'no' as Answer
)

-- select * from aggregated

select Id, yes_no_to_boolean(Answer) as Result from bool_test