{%- macro clean_all_objects() -%}

{%- set trunc_statement -%}

        TRUNCATE TABLE {{ target.schema }}.{{ var('process_log') }}

{%- endset -%}

{%- do run_query(trunc_statement) -%}

{%- set all_objects -%}

    with 
    
    final as (
    
        select 
            case when table_type = 'BASE TABLE' 
                then 'TABLE' 
                else 'VIEW'
            end as obj_type,
            table_schema as obj_schema,
            table_name as obj_name
        from 
            svv_tables 
        where true and
            table_schema = '{{ target.schema }}'
            and table_schema <> 'ds_prod' 
            and table_name not in ('process_log', 'rv_schema_name__table_name')
            and table_name not like 'save%'

    )

    select * from final

{%- endset -%}

{%- set drop_objects = dbt_utils.get_query_results_as_dict(all_objects) -%}

{% for obj_type in drop_objects["obj_type"] -%}

    {%- set drop_statement -%}

        DROP {{ obj_type }} {{ drop_objects.obj_schema[loop.index0] }}.{{ drop_objects.obj_name[loop.index0] }}

    {%- endset -%}

    {%- do run_query(drop_statement) -%}
     
{%- endfor -%}

{%- endmacro -%}
