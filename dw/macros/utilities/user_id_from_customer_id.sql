{% macro user_id_from_customer_id(customer_id, alias='user_id') -%}
    case
        when len(regexp_substr(split_part({{ customer_id }}, '_', 2), '\\d+$')) > 0
            then regexp_substr(split_part({{ customer_id }}, '_', 2), '\\d+$') :: integer
        else null
    end as {{ alias }}
{%- endmacro %}