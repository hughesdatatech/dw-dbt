{%- macro build_hash_value(value, alias='null') -%}
    cast(sha2({{ value }}, 256) as varbinary(64)) {{ alias if alias != 'null' }}
{%- endmacro -%}
