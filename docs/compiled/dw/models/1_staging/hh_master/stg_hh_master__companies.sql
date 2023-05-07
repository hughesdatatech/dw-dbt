select
    'default' as rv_hh_master__companies_tenant_key,
    cast(sha2(nvl(trim(cast(id as varchar)), ''), 256) as varbinary(64)) as rv_hh_master__companies_hk,
    'ref_hh_master__companies.csv' as rv_hh_master__companies_rec_source,
    'ff1d7726-239d-4381-90cf-69aac5d69bb2' as rv_hh_master__companies_job_id_job_id,
    'circleci' as rv_hh_master__companies_job_user_id,
    'default' as rv_hh_master__companies_jira_task_key,
    to_char(timestamp '2023-05-07 20:09:35.462905+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp   as rv_hh_master__companies_extracted_at,
    to_char(timestamp '2023-05-07 20:09:35.462905+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp  as rv_hh_master__companies_loaded_at,
    cast(sha2(nvl(trim(cast(id as varchar)), ''), 256) as varbinary(64)) as rv_hh_master__companies_hd,
    id,
    name
from 
    dw_dev.dbt_steve.ref_hh_master__companies