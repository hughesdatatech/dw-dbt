with

exceptions as (

    {%- for rule in var('member_exception_rules') %}
    select
        *,
        '{{ var("member_exception_rules")[rule] }}' as exception 
    from 
        {{ ref('br_member') }}
    where true
        and {{ var("member_exception_rules")[rule] }}
    {% if not loop.last %} union all {% endif -%}
    {% endfor %}

)

select
    member_tenant_key,
    member_hk,
    member_rec_source,
    member_job_id,
    member_job_user_id,
    member_jira_task_key,
    member_extracted_at,
    member_loaded_at,
    member_hd,
    member_id,
    first_name,
    last_name,
    birth_date,
    company_name,
    last_active_at,
    score,
    joined_at,
    state_code,
    listagg(exception, ' | ') as exception_list
from 
    exceptions
where true
    {{ dbt_utils.group_by(n=18) }}
