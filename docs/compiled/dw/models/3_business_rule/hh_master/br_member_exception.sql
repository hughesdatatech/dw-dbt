with

exceptions as (
    select
        *,
        'joined_at < birth_date' as exception 
    from 
        dw_dev.dbt_steve.br_member
    where true
        and joined_at < birth_date
     union all 
    select
        *,
        'last_active_at < birth_date' as exception 
    from 
        dw_dev.dbt_steve.br_member
    where true
        and last_active_at < birth_date
     union all 
    select
        *,
        'last_active_at < joined_at' as exception 
    from 
        dw_dev.dbt_steve.br_member
    where true
        and last_active_at < joined_at
    

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
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18