{% macro treat_char_attribute(attribute) -%}
    trim({{ attribute }}) as {{ attribute }}
{%- endmacro %}