with

final as (

    select
        
        'default' as rv_hh_unity_golf_club__members_tenant_key,
        'ref_hh_unity_golf_club__members.csv' as rv_hh_unity_golf_club__members_rec_source,
        '7aff3c53-f4b7-4d61-b4bf-0d43d20f97b0' as rv_hh_unity_golf_club__members_job_id,
        'circleci' as rv_hh_unity_golf_club__members_job_user_id,
        'default' as rv_hh_unity_golf_club__members_jira_task_key,
        to_char(timestamp '2023-05-07 19:52:53.314653+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp   as rv_hh_unity_golf_club__members_extracted_at,
        to_char(timestamp '2023-05-07 19:52:53.314653+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp  as rv_hh_unity_golf_club__members_loaded_at,
        cast(sha2(nvl(trim(cast(id as varchar)), ''), 256) as varbinary(64)) as rv_hh_unity_golf_club__members_hd,
        id,
        first_name,
        last_name,
        to_date(dob, 'yyyy/mm/dd') as dob,
        company_id,
        to_date(last_active, 'yyyy/mm/dd') as last_active,
        score,
        date_from_parts(member_since, 1, 1) as member_since,
        state
    from 
        dw_dev.dbt_steve.ref_hh_unity_golf_club__members

)

select
    cast(sha2(nvl(trim(cast(id as varchar)), '') || rv_hh_unity_golf_club__members_rec_source, 256) as varbinary(64)) as rv_hh_unity_golf_club__members_hk,
    *
from 
    final