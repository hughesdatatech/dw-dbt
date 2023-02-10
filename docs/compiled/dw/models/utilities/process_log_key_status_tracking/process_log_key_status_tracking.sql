

with 

all_keys_w_stat as (


      
    select
        *,
        row_number() over(partition by rv___hk, rec_source order by loaded_at desc) as row_sqn
    from 
        dw_dev.dbt_steve.process_log_key_status_tracking
    where true



),

latest_key_stat as (

    select *
    from 
        all_keys_w_stat
    where true
        and row_sqn = 1

),



    

    pagov__opioid_stays_rv as (

        select
            rv_pagov__opioid_stays_hk,
            dbt_scd_id,
            row_number() over(partition by rv_pagov__opioid_stays_hk order by dbt_updated_at desc) as row_sqn
        from 
            dw_dev.dbt_steve.rv_pagov__opioid_stays

    ),

    pagov__opioid_stays_latest_rv as (

        select *
        from 
            pagov__opioid_stays_rv
        where true
            and row_sqn = 1

    ),

    pagov__opioid_stays_stat as (

        -- The purpose here is to flag and ultimately track first-time key insertions, deletions, and re-insertions to the raw vault (key = rv_[schema_name__table_name]_hk).
        -- If a key is not flagged it means the raw vault record was an update in which case we don't insert any record to the key status tracking table. 
        select 
            'default' as rv_pagov__opioid_stays_tenant_key,
    cast(sha2(nvl(trim(cast(latest_rv.rv_pagov__opioid_stays_hk as varchar)), ''), 256) as varbinary(64)) rv_pagov__opioid_stays_hk,
    
        'rv_pagov__opioid_stays' as rv_pagov__opioid_stays_rec_source,
    
    '4def11eb-fd99-4051-9ba7-2b58997c53a2' as rv_pagov__opioid_stays_job_id,
    'circleci' as rv_pagov__opioid_stays_job_user_id,
    'default' as rv_pagov__opioid_stays_jira_task_key,
    to_char(timestamp '2023-02-10 01:55:24.508211+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp  as rv_pagov__opioid_stays_extracted_at,
    to_char(timestamp '2023-02-10 01:55:24.508211+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp rv_pagov__opioid_stays_loaded_at,
            latest_rv.rv_pagov__opioid_stays_hk as rv___hk,
            latest_rv.dbt_scd_id as rv_dbt_scd_id,
            case
                when
                    ls.rv_key_status is null -- Raw vault record has never been tracked so it's a new insert.
                    then 'new_insert'
                when stg.rv_pagov__opioid_stays_hk is null and False
                    and nvl(ls.rv_key_status, '') <> 'deleted'
                    then 'delete' -- Latest raw vault record IS NOT in staging and latest status is not deleted, so the action is a delete.
                when stg.rv_pagov__opioid_stays_hk is not null and False
                    and nvl(ls.rv_key_status, '') = 'deleted'
                    then 're_insert' -- Latest raw vault records IS in staging and latest status is deleted, so the action is a re-insert.
                -- Otherwise it means the action is an update in which case we ignore.
            end as _rv_key_action,
            case
                when
                    _rv_key_action in ('new_insert', 're_insert')
                    then 'active'
                when
                    _rv_key_action = 'delete'
                    then 'deleted'
            end as _rv_key_status
        from 
            pagov__opioid_stays_latest_rv as latest_rv
            left join dw_dev.dbt_steve.stg_pagov__opioid_stays stg
                on latest_rv.rv_pagov__opioid_stays_hk = stg.rv_pagov__opioid_stays_hk
            left join latest_key_stat ls on
                ls.rv___hk = latest_rv.rv_pagov__opioid_stays_hk
                and ls.rec_source = 'rv_' || 'pagov__opioid_stays'

    )
    

    







    

        select 
            rv_pagov__opioid_stays_tenant_key as tenant_key,
            rv_pagov__opioid_stays_hk as hk,
            rv_pagov__opioid_stays_rec_source as rec_source,
            rv_pagov__opioid_stays_job_id as job_id,
            rv_pagov__opioid_stays_job_user_id as job_user_id,
            rv_pagov__opioid_stays_jira_task_key as jira_task_key,
            rv_pagov__opioid_stays_extracted_at as extracted_at,
            rv_pagov__opioid_stays_loaded_at as loaded_at,
            rv___hk,
            rv_dbt_scd_id,
            _rv_key_action as rv_key_action,
            _rv_key_status as rv_key_status,
            loaded_at as rv_key_status_detected_at
        from 
            pagov__opioid_stays_stat
        where true
            and _rv_key_action is not null

    

    



