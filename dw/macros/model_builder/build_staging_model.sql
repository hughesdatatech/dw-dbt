{%- macro build_staging_model(stg_model_name) -%}

{# Parse schema, table, and key values from the staging model name. #}
{%- set source_schema__table = get_source_schema__table(stg_model_name) -%}
{%- set source_schema = get_source_schema(source_schema__table) -%}
{%- set source_table = get_source_table(source_schema__table) -%}
{%- set unique_key = get_model_config(stg_model_name, 'unique_key') -%}
{%- set source_table_relation = source(source_schema, source_table) -%}

{# Get strategy, default exception columns, and order by columns applicable to the schema. #}
{%- set snapshot_strategy = get_schema_config(source_schema, 'snapshot_strategy') -%}
{%- set staging_model_default_exception_cols = get_schema_config(source_schema, 'staging_model_default_exception_cols') -%}
{%- if staging_model_default_exception_cols is none -%}
    {%- set staging_model_default_exception_cols = [] -%}
{%- endif %}
{%- set staging_model_order_by_cols = get_schema_config(source_schema, 'staging_model_order_by_cols') -%}
{%- set build_hd = true -%}
{%- set extracted_at_column = 'null' -%}
{%- if snapshot_strategy == 'timestamp' -%}
    {%- set build_hd = false -%}
    {%- set extracted_at_column = var("extracted_at_default") -%}
{%- endif %}

{# Get special case boolean and reserved word colums applicable to the model. #}
{%- set namesafe_cols = [] -%}
{%- set boolean_namesafe_cols=[] -%}
{%- set boolean_cols = get_model_config(stg_model_name, 'boolean_cols') -%}
{%- if boolean_cols is none -%}
    {%- set boolean_cols = [] -%}
{%- endif %}
{%- set reserved_cols = get_model_config(stg_model_name, 'reserved_cols') -%}
{%- set boolean_reserved_cols = get_model_config(stg_model_name, 'boolean_reserved_cols') -%}

{# Treat reserved word colums and boolean reserved word columns with double quotes #}
{%- if reserved_cols is none -%} 
    {% set reserved_cols = [] -%}
{%- else -%}
    {%- for val in reserved_cols -%}
        {%- do namesafe_cols.append('"' + val + '"') -%}
    {%- endfor -%}
{%- endif -%}
{%- if boolean_reserved_cols is none -%}
    {%- set boolean_reserved_cols = [] -%}
{%- else -%}
    {%- for val in boolean_reserved_cols -%}
        {%- do boolean_namesafe_cols.append('"' + val + '"') -%}
    {%- endfor -%}
{%- endif -%}

{# 
    Get the columns to be selected in the final output, excluding any default exception columns, plus any reserved and reserved boolean columns. 
    The reserved and reserved boolean columns will be included in the output via their respective namesafe lists, where they've been treated to make them selectable.
#}
{%- set select_cols = dbt_utils.get_filtered_columns_in_relation(source_table_relation, except=staging_model_default_exception_cols + reserved_cols + boolean_reserved_cols) -%}

with

all_source_rows as (

    select
        {{
            build_dw_metadata_cols(
                source_schema=source_schema,
                source_table=source_table,
                unique_key=unique_key,
                extracted_at_column=extracted_at_column,
                build_hd=build_hd,
                boolean_cols=boolean_cols + boolean_namesafe_cols,
                reserved_cols=reserved_cols + boolean_reserved_cols,
                namesafe_cols=namesafe_cols,
                jira_task_key='null',
                alias='rv_' + source_schema__table
            )  
        }},
        {{
            select_cols | join(', \n\t\t')
        }},
        {{ namesafe_cols | join(', \n\t\t') + ',' if namesafe_cols | length > 0 }}
        {{ boolean_namesafe_cols | join(', \n\t\t') + ',' if boolean_namesafe_cols | length > 0 }}
        row_number() over (
            partition by {{ unique_key | join(',') }}
            order by {{ staging_model_order_by_cols | join(',') }}
        ) as row_num
    from 
        {{ source_table_relation }}
        
)

select
    {{ select_dw_metadata_cols(prefix='rv', alias=source_schema__table, hd=build_hd) }},
    {{ select_cols | join(', \n\t') }}
    {{ ',' + namesafe_cols | join(', \n\t\t') if namesafe_cols | length > 0 }}
    {{ ',' + boolean_namesafe_cols | join(', \n\t\t') if boolean_namesafe_cols | length > 0 }}
from 
    all_source_rows
where true
    and row_num = 1 {# Dedupe based on latest record #}
{{ add_test_row_limit() }}

{%- endmacro -%}
