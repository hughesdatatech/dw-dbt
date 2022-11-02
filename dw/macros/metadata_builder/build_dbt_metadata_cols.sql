{% macro build_dbt_metadata_cols(prefix) -%}
    dbt_scd_id as {{ prefix ~ '_dbt_scd_id' }},
    dbt_updated_at as {{ prefix ~ '_dbt_updated_at' }},
    dbt_valid_from as {{ prefix ~ '_dbt_valid_from' }},
    dbt_valid_to as {{ prefix ~ '_dbt_valid_to' }}
{%- endmacro %}