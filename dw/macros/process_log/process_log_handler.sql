{% macro process_log_handler(object_type='dbt_project', event_name='unknown', sequence_description='unknown') %}

    {%- if sequence_description == 'on_run_start' or sequence_description == 'on_run_end' -%}

        {%- set create_query -%}

            create table if not exists {{ target.schema }}.{{ var('process_log') }} (
                pipeline_metadata varchar,
                pipeline_project_metadata varchar,
                pipeline_schedule_metadata varchar,
                job_id varchar(500),  
                object_type varchar(100),
                object_identifier varchar(200),
                event_name varchar(100),
                sequence_description varchar(100),
                log_info1 varchar, 
                log_info2 varchar,
                loaded_at timestamp, 
                inserted_at timestamp
            )

        {%- endset -%}

        {%- do run_query(create_query) -%}

    {%- endif -%}

    {%- if var("run_key") == var("clean_all_objects_key") -%}

        {{ clean_all_objects() }}

    {%- endif -%}

     {%- if var('object_type_override') != 'null' -%}
        {%- set object_type = var('object_type_override') -%}
    {%- endif -%}

    {%- set object_identifier = 'unknown' -%}
    {%- if object_type == 'dbt_project' -%}
        {%- set object_identifier = project_name -%}
    {%- elif object_type == 'model' or object_type == 'snapshot' -%}
        {%- set object_identifier = this -%}
    {%- elif object_type == 'pipeline' -%}
        {%- set object_identifier = var('object_identifier_override') -%}
    {%- endif -%}

    {%- if var('event_name_override') != 'null' -%}
        {%- set event_name = var('event_name_override') -%}
    {%- endif -%}

    {%- if var('sequence_description_override') != 'null' -%}
        {%- set sequence_description = var('sequence_description_override') -%}
    {%- endif -%}

    {%- set insert_query -%}

        insert into {{ target.schema }}.{{ var('process_log') }} (
            pipeline_metadata,
            pipeline_project_metadata,
            pipeline_schedule_metadata,
            job_id, 
            object_type,
            object_identifier,
            event_name,
            sequence_description,
            log_info1,
            log_info2,
            loaded_at, 
            inserted_at
        ) 
        values(
            '{{ var("pipeline_metadata") }}',
            '{{ var("pipeline_project_metadata") }}',
            '{{ var("pipeline_schedule_metadata") }}',
            '{{ invocation_id }}', 
            '{{ object_type }}',
            '{{ object_identifier }}',
            '{{ event_name }}',
            '{{ sequence_description }}',
            {{ "'dbt_version: " + dbt_version + "'" }},
            null,
            {{ build_loaded_at('null') }}, 
            {{ build_inserted_at('null') }}
        )
    
    {%- endset -%}
    
    {%- do run_query(insert_query) -%}

{% endmacro %}
