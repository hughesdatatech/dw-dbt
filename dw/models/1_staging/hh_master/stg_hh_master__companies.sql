select
    'default' as rv_hh_master__companies_tenant_key,
    cast(sha2(nvl(trim(cast(id as varchar)), ''), 256) as varbinary(64)) as rv_hh_master__companies_hk,
    'ref_hh_master__companies.csv' as rv_hh_master__companies_rec_source,
    {{ build_job_id(invocation_id, 'rv_hh_master__companies_job_id') }},
    'circleci' as rv_hh_master__companies_job_user_id,
    'default' as rv_hh_master__companies_jira_task_key,
    {{ build_loaded_at(alias='null') }}  as rv_hh_master__companies_extracted_at,
    {{ build_loaded_at(alias='null') }} as rv_hh_master__companies_loaded_at,
    cast(sha2(nvl(trim(cast(id as varchar)), ''), 256) as varbinary(64)) as rv_hh_master__companies_hd,
    id,
    name
from 
    {{ ref('ref_hh_master__companies') }}
