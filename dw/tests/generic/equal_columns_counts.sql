{%- test equal_column_counts(model, compare_model, exclude_meta_columns=True) -%}
{{ config(severity = 'warn') }}
{%- if execute -%}

{%- set meta_prefix = model.name | replace('stg_', 'rv_') if model.name.startswith('stg_') else model.name -%}

{%- set meta_columns =
                    [
                        '_dms_operation',
                        meta_prefix + '_tenant_key', 
                        meta_prefix + '_hk', 
                        meta_prefix + '_rec_source',
                        meta_prefix + '_job_id', 
                        meta_prefix + '_job_user_id', 
                        meta_prefix + '_jira_task_key',
                        meta_prefix + '_extracted_at', 
                        meta_prefix + '_loaded_at', 
                        meta_prefix + '_hd',
                        'stg_row_num'
                    ] -%}
{%- set exclude_columns = meta_columns if exclude_meta_columns else [] -%}

{%- set number_columns = dbt_utils.get_filtered_columns_in_relation(model,
    exclude_columns) | length -%}

{%- set compare_number_columns = dbt_utils.get_filtered_columns_in_relation(
    compare_model, exclude_columns)| length -%}

with test_data as (

    select
        {{ number_columns }} as number_columns,
        {{ compare_number_columns }} as compare_number_columns

)
select *
from test_data
where
    number_columns != compare_number_columns

{%- endif -%}
{%- endtest -%}
