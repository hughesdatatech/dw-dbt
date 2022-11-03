{% macro select_dbt_metadata_cols(prefix='null', use_null_value=False) -%}
    {% if use_null_value==True %}
        null::varbinary(64) as {{ prefix ~ '_key' if prefix != 'null' else '_dw_internal_key' }},
        null::varchar(32) as {{ prefix ~ '_dbt_scd_id' if prefix != 'null' else 'dbt_scd_id' }},
        null::timestamp as {{ prefix ~ '_dbt_updated_at' if prefix != 'null' else 'dbt_updated_at' }},
        null::timestamp as {{ prefix ~ '_dbt_valid_from' if prefix != 'null' else 'dbt_valid_from' }},
        null::timestamp as {{ prefix ~ '_dbt_valid_to' if prefix != 'null' else 'dbt_valid_to' }}
    {% else %}
        {{ prefix ~ '_key' if prefix != 'null' else '_dw_internal_key' }},
        {{ prefix ~ '_dbt_scd_id' if prefix != 'null' else 'dbt_scd_id' }},
        {{ prefix ~ '_dbt_updated_at' if prefix != 'null' else 'dbt_updated_at' }},
        {{ prefix ~ '_dbt_valid_from' if prefix != 'null' else 'dbt_valid_from' }},
        {{ prefix ~ '_dbt_valid_to' if prefix != 'null' else 'dbt_valid_to' }}
    {% endif%}
{%- endmacro %}