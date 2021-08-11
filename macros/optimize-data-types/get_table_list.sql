{% macro get_table_list(project_id, dataset, target_project_id, target_dataset) %}
    {# this #}
    {% set get_table_query %}
        SELECT table_id as table_id FROM {{project_id}}.{{dataset}}.__TABLES__ limit 0;
    {% endset %}
​
    {% set results = run_query(get_table_query) %}
​
    {% if execute %}
        {% set table_list = results.columns[0].values() %}
            {% for tbl in table_list %}
                {% set get_columns_query %}
                    SELECT 
                        column_name, data_type
                    FROM
                        {{project_id}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS
                    WHERE
                        table_name='{{tbl}}'
                        AND (data_type = 'BOOLEAN' OR 
                             data_type = 'FLOAT' OR
                             data_type = 'INTEGER' OR
                             data_type = 'NUMERIC'  )
                {% endset %}
                
                {% set column_results = run_query(get_columns_query)%}
                
                {% if execute %}
                    {% set column_list = column_results.columns[0].values() %}
                        {% set proposed_column_query %}
                            {% for col in column_list%}
                                {{find_proposed_column_for_numbers(col, project_id, dataset, tbl )}}
                                {{' union all '}} {{loop.counter}}
                            {% endfor %}
                                SELECT 
                                    column_name, 
                                    data_type AS proposed_data_type
                                FROM
                                    {{project_id}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS
                                WHERE
                                    table_name='{{tbl}}'
                                    AND (data_type not in ('BOOLEAN','FLOAT','INTEGER','NUMERIC'))
                        {% endset %}
                        {%set proposed_columns_results = run_query(proposed_column_query)%}
                            {% set create_table_query%}
                                create table {{target_project_id}}.{{target_dataset}}.{{tbl}} (
                                    {% for val in proposed_columns_results %}
                                        {{ val[0]}} {{val[1]}} ,
                                    {% endfor %}
                                )
                            {% endset %}
                            {{ log("Running some_macro: " ~ create_table_query ~ ", " ) }}
                            {% set execute_object = run_query(create_table_query) %}
                {% else %}
                    {% set column_list = [] %}
                {% endif %}
            {% endfor %}
    {% else %}
        {% set table_list = [] %}
    {% endif %}
{% endmacro %}