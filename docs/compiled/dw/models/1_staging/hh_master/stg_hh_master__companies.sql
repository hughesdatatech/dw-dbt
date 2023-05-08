select
    'default' as rv_hh_master__companies_tenant_key,
    cast(sha2(nvl(trim(cast(id as varchar)), ''), 256) as varbinary(64)) as rv_hh_master__companies_hk,
    'ref_hh_master__companies.csv' as rv_hh_master__companies_rec_source,
    '587a075a-8880-4f2d-b81d-a019d9ffd952' as rv_hh_master__companies_job_id_job_id,
    'circleci' as rv_hh_master__companies_job_user_id,
    'default' as rv_hh_master__companies_jira_task_key,
    to_char(timestamp '2023-05-08 20:34:55.122195+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp   as rv_hh_master__companies_extracted_at,
    to_char(timestamp '2023-05-08 20:34:55.122195+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp  as rv_hh_master__companies_loaded_at,
    cast(sha2(nvl(trim(cast(id as varchar)), ''), 256) as varbinary(64)) as rv_hh_master__companies_hd,
    id,
    name
from 
    dw_dev.dbt_steve.ref_hh_master__companies