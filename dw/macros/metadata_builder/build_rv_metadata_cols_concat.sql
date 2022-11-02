{% macro build_rv_metadata_cols_concat(alias) -%}
    'rv_{{ alias }}_hk = ' || nvl(rv_{{ alias }}_hk::varchar, '') || '; ' ||
    'rv_{{ alias }}_loaded_at = ' || nvl(rv_{{ alias }}_loaded_at::varchar, '') || '; ' ||
    'rv_{{ alias }}_dbt_scd_id = ' || nvl(rv_{{ alias }}_dbt_scd_id::varchar, '') || '; ' ||
    'rv_{{ alias }}_dbt_updated_at = ' || nvl(rv_{{ alias }}_dbt_updated_at::varchar, '') || '; ' ||
    'rv_{{ alias }}_dbt_valid_from = ' || nvl(rv_{{ alias }}_dbt_valid_from::varchar, '') || '; ' ||
    'rv_{{ alias }}_dbt_valid_to = ' || nvl(rv_{{ alias }}_dbt_valid_to::varchar, '') || '; ' ||
    'rv_{{ alias }}_key_action = ' || nvl(rv_{{ alias }}_key_action, '') || '; ' ||
    'rv_{{ alias }}_key_status = ' || nvl(rv_{{ alias }}_key_status, '') || '; ' ||
    'rv_{{ alias }}_key_status_detected_at = ' || nvl(rv_{{ alias }}_key_status_detected_at::varchar, '') || '; ' ||
    'br_{{ alias }}_row_sqn_desc = ' || nvl(br_{{ alias }}_row_sqn_desc::varchar, '') as rv_{{ alias }}_metadata
{%- endmacro -%}