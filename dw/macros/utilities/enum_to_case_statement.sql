{% macro enum_to_case_stmt(column_name, enum_map, else_value=None) -%}
    case
    {%- for _id, name in enum_map.items() %}
        when {{ column_name }} = {{ _id }} 
            then '{{ name }}'
    {%- endfor %}
        else {{ wrap_in_quotes(else_value) if else_value else 'null' }}
    end
{%- endmacro %}
