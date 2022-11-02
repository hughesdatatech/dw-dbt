{% macro build_dw_metadata_cols_concat(alias) -%}
    '{{ alias }}_tenant_key = ' || nvl({{ alias }}_tenant_key, '') || '; ' ||
    '{{ alias }}_hk = ' || nvl({{ alias }}_hk::varchar, '') || '; ' ||
    '{{ alias }}_job_id = ' || nvl({{ alias }}_job_id, '') || '; ' ||
    '{{ alias }}_job_user_id = ' || nvl({{ alias }}_job_user_id, '') || '; ' ||
    '{{ alias }}_jira_task_key = ' || nvl({{ alias }}_jira_task_key, '') || '; ' ||
    '{{ alias }}_extracted_at = ' || nvl({{ alias }}_extracted_at::varchar, '') || '; ' ||
    '{{ alias }}_loaded_at = ' || nvl({{ alias }}_loaded_at::varchar, '') as {{ alias }}_metadata
{%- endmacro -%}