{%- macro build_snapshot_model(snapshot_model_name) -%}

{# Parse schema and snapshot strategy from the snapshot model name. #}
{%- set source_schema__table = get_source_schema__table(snapshot_model_name) -%}
{%- set source_schema = get_source_schema(source_schema__table) -%}
{%- set snapshot_strategy = get_schema_config(source_schema, 'snapshot_strategy') -%}

{%- if snapshot_strategy == 'timestamp' -%}
    
    {%- set snapshot_updated_at_col =  get_schema_config(source_schema, 'snapshot_updated_at_col') -%}
    
    {{
        config(
            target_schema=target.get('schema'),
            unique_key=snapshot_model_name + '_hk',
            strategy='timestamp',
            updated_at=snapshot_updated_at_col,
            invalidate_hard_deletes=True,
        )
    }}

    select * from {{ ref('stg_' + source_schema__table) }}

{%- elif snapshot_strategy == 'check' -%}

    {{
        config(
            target_schema=target.get('schema'),
            unique_key=snapshot_model_name + '_hk',
            strategy='check',
            check_cols=[snapshot_model_name + '_hd'],
            invalidate_hard_deletes=True,
        )
    }}

    select * from {{ ref('stg_' + source_schema__table) }}

{%- endif -%}

{%- endmacro -%}
