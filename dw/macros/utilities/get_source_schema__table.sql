{%- macro get_source_schema__table(model_name) -%}
    {%- set source_schema__table = '' -%}

    {%- if model_name.startswith('stg_') -%}
        {%- set source_schema__table = model_name.replace('stg_', '') -%}
    {%- elif model_name.startswith('rv_') -%}
        {%- set source_schema__table = model_name.replace('rv_', '') -%}
    {%- endif -%}

    {{ return(source_schema__table) }}
{%- endmacro -%}
