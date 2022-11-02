{% macro treat_num_pii_attribute(attribute) -%}
    {{ attribute }} as {{ attribute }}
{%- endmacro %}
