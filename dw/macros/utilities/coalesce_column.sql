{% macro coalesce_column(column, replace_with="''", alias='same_as_column') -%}
    coalesce({{ column }}, {{ replace_with }}) as {{ column if alias == 'same_as_column' else alias }}
{%- endmacro %}