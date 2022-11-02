{% macro treat_char_pii_attribute(attribute) -%}
    {{ treat_char_attribute(attribute) }}
{%- endmacro %}
