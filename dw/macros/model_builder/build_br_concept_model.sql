{%- macro build_br_concept_model(concept_model_name, base_model_name, jira_task_key) -%}

{%- set concept_name = concept_model_name.replace('br_', '') %}
{%- set base_name = base_model_name.replace('br_', '') %}

select
    '{{ jira_task_key }}' as jira_task_key,
    br_{{ base_name }}_hk as br_{{ concept_name }}_hk,
    {{ dbt_utils.get_filtered_columns_in_relation(from=ref(base_model_name), except=[base_model_name + '_hk']) | join(',') }}
from 
    {{ ref(base_model_name) }}
where true
    and br_{{ base_name }}_row_sqn_desc = 1

{%- endmacro %}
