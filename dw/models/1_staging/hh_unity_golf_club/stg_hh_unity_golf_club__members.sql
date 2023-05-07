with

final as (

    select
        {#
            TO DO: leverage macros so that metadata and column names are more auto-generated.
        #}
        'default' as rv_hh_unity_golf_club__members_tenant_key,
        'ref_hh_unity_golf_club__members.csv' as rv_hh_unity_golf_club__members_rec_source,
        {{ build_job_id(invocation_id, 'rv_hh_unity_golf_club__members') }},
        'circleci' as rv_hh_unity_golf_club__members_job_user_id,
        'default' as rv_hh_unity_golf_club__members_jira_task_key,
        {{ build_loaded_at(alias='null') }}  as rv_hh_unity_golf_club__members_extracted_at,
        {{ build_loaded_at(alias='null') }} as rv_hh_unity_golf_club__members_loaded_at,
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
        {{ ref('ref_hh_unity_golf_club__members') }}

)

select
    cast(sha2(nvl(trim(cast(id as varchar)), '') || rv_hh_unity_golf_club__members_rec_source, 256) as varbinary(64)) as rv_hh_unity_golf_club__members_hk,
    *
from 
    final
