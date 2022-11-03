{% macro build_inserted_at(alias='inserted_at') -%}
    to_char(sysdate(), 'YYYY-MM-DD HH24:MI:SS')::timestamp {{ alias if alias != 'null' }}
{%- endmacro %}