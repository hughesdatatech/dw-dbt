

select
    'default' as im_opioid_stay_tenant_key,
    cast(sha2(nvl(trim(cast(br_opioid_stay_hk as varchar)), ''), 256) as varbinary(64)) im_opioid_stay_hk,
    
        'dw_dev.dbt_steve.br_opioid_stay' as im_opioid_stay_rec_source,
    
    '587a075a-8880-4f2d-b81d-a019d9ffd952' as im_opioid_stay_job_id,
    'circleci' as im_opioid_stay_job_user_id,
    jira_task_key as im_opioid_stay_jira_task_key,
    to_char(timestamp '2023-05-08 20:34:55.122195+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp  as im_opioid_stay_extracted_at,
    to_char(timestamp '2023-05-08 20:34:55.122195+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp im_opioid_stay_loaded_at,
    'im_opioid_stay_tenant_key = ' || nvl(im_opioid_stay_tenant_key, '') || '; ' ||
    'im_opioid_stay_hk = ' || nvl(im_opioid_stay_hk::varchar, '') || '; ' ||
    'im_opioid_stay_job_id = ' || nvl(im_opioid_stay_job_id, '') || '; ' ||
    'im_opioid_stay_job_user_id = ' || nvl(im_opioid_stay_job_user_id, '') || '; ' ||
    'im_opioid_stay_jira_task_key = ' || nvl(im_opioid_stay_jira_task_key, '') || '; ' ||
    'im_opioid_stay_extracted_at = ' || nvl(im_opioid_stay_extracted_at::varchar, '') || '; ' ||
    'im_opioid_stay_loaded_at = ' || nvl(im_opioid_stay_loaded_at::varchar, '') as im_opioid_stay_metadata,
    *
from 
    dw_dev.dbt_steve.br_opioid_stay