{% macro build_loaded_at(alias='') -%}
    to_char(timestamp '{{ run_started_at }}', 'YYYY-MM-DD HH24:MI:SS')::timestamp {{ alias + '_loaded_at' if alias != 'null' }}
{%- endmacro %}