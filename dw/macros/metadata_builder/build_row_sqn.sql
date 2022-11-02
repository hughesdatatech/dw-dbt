{% macro build_row_sqn(alias_prefix='null', partition_list='', order_by='loaded_at desc') -%}
    row_number() over(partition by {{ partition_list }} order by {{ order_by }}) {{ alias_prefix if alias_prefix != 'null' }}
{%- endmacro %}