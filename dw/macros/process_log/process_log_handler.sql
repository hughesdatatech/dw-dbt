{% macro process_log_handler(component='unknown', job_step='unknown') %}

    {%- if job_step == 'on-run-start' or job_step == 'on-run-end' -%}

        {%- set create_query -%}

            create table if not exists {{ target.schema }}.{{ var('process_log') }} (
                pipeline_metadata varchar,
                pipeline_project_metadata varchar,
                pipeline_schedule_metadata varchar,
                job_id varchar(500), 
                job_step_component varchar(500), 
                job_step varchar(1000), 
                job_step_info1 varchar, 
                job_step_info2 varchar,
                loaded_at timestamp, 
                inserted_at timestamp
            )

        {%- endset -%}

        {%- do run_query(create_query) -%}

    {%- endif -%}

    {%- if var("run_key") == var("clean_all_objects_key") -%}

        {{ clean_all_objects() }}

    {%- endif -%}

    {%- set insert_query -%}

        insert into {{ target.schema }}.{{ var('process_log') }} (
            pipeline_metadata,
            pipeline_project_metadata,
            pipeline_schedule_metadata,
            job_id, 
            job_step_component, 
            job_step, 
            job_step_info1,
            job_step_info2,
            loaded_at, 
            inserted_at
        ) 
        values(
            '{{ var("pipeline_metadata") }}',
            '{{ var("pipeline_project_metadata") }}',
            '{{ var("pipeline_schedule_metadata") }}',
            '{{ invocation_id }}', 
            {{ "'dbt project: " + project_name + "'" if component == 'job' else remove_double_quotes(this) }},
            '{{ component + ": " + job_step }}',
            {{ "'dbt version: " + dbt_version + "'" if component == 'job' else 'null' }},
            {{ "'clean_all_objects: complete'" if var("run_key") == var("clean_all_objects_key") else 'null' }},
            {{ build_loaded_at('null') }}, 
            {{ build_inserted_at('null') }}
        )
    
    {%- endset -%}
    
    {%- do run_query(insert_query) -%}

{% endmacro %}
