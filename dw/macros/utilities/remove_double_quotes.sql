{%- macro remove_double_quotes(value) -%}
    '{{ (value|string).replace("\"", '') }}'
{%- endmacro -%}