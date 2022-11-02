{%- macro get_model_config(stg_model_name, config_type) -%}
    {%- set source_schema__table = get_source_schema__table(stg_model_name) -%}
    {%- set source_schema = get_source_schema(source_schema__table) -%}
    {%- set model_config_key = 'stg_' + source_schema + '__model_config' -%}

    {{ return(var(model_config_key)[source_schema__table][config_type]) }}
{%- endmacro -%}
