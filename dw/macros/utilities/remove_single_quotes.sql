{% macro remove_single_quotes(value) %}
    '{{ (value|string).replace("'", "") }}'
{% endmacro %}
