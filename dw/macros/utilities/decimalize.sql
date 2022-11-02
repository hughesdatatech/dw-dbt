{% macro decimalize(column, divide_by=100.00, round_to=2, alias='same_as_column') -%}
    round(coalesce({{ column }}, 0) / {{ divide_by }}, {{ round_to }}) as {{ column if alias == 'same_as_column' else alias }}
{%- endmacro %}