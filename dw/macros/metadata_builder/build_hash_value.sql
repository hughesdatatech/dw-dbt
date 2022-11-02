{%- macro build_hash_value(value, alias='null') -%}
    cast(sha2({{ value }}, 256) as varbyte(64)) {{ alias if alias != 'null' }}
{%- endmacro -%}
