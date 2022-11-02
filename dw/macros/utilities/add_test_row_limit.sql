{% macro add_test_row_limit() -%}
    {{ 'limit ' + var("test_row_limit")|string if var("test_row_limit") > 0 }}
{%- endmacro %}