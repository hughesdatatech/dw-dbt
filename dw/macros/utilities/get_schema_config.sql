{%- macro get_schema_config(source_schema, config_type) -%}
    {{ return(var('stg_schema_config')[source_schema][config_type]) }}
{%- endmacro -%}
