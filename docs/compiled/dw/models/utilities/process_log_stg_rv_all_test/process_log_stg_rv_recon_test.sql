with __dbt__cte___stg_rv_recon_test as (


with



    

        test_pagov__opioid_stays as (
                

with 
SAT_UKEY_SGTG_stg_pagov__opioid_stays
as (
  select 
    count(1) as unique_key_tgt_not_loaded,
    'county_name,time_period_date_end,rv_pagov__opioid_stays_hd' as unique_key_stg_columns,
    'county_name,time_period_date_end,rv_pagov__opioid_stays_hd' as unique_key_tgt_columns
  from 
    dw_dev.dbt_steve.stg_pagov__opioid_stays as sg 
  where not exists
  (
    select 
      1
    from 
      dw_dev.dbt_steve.rv_pagov__opioid_stays as tg
    where 
      nvl(sg.county_name::varchar, '') = nvl(tg.county_name::varchar, '') and nvl(sg.time_period_date_end::varchar, '') = nvl(tg.time_period_date_end::varchar, '') and nvl(sg.rv_pagov__opioid_stays_hd::varchar, '') = nvl(tg.rv_pagov__opioid_stays_hd::varchar, '') 
      and dbt_valid_to is null
      /*
        For the check strategy, hard-deletes can't be invalidated. You want to compare the staged data against the latest target (raw vault) records.
        This will handle a situation (unlikely?) where data changed but then changed back to a prior value, e.g. a to b back to a.
        The latest records when using the check strategy are identifiable where dbt_valid_to is null.
        For the timestamp strategy

      */ 
  )
), 
FETCH_SAT_STATS1_stg_pagov__opioid_stays
as (
  select
    nvl(sum(cnt), 0) as unique_key_stg_gross,
    count(1) as unique_key_stg_unique
  from (
    select
      count(*) as cnt
    from 
      dw_dev.dbt_steve.stg_pagov__opioid_stays
    group by
      county_name,time_period_date_end,rv_pagov__opioid_stays_hd
  )
), 
FETCH_SAT_STATS2_stg_pagov__opioid_stays
as (
    select 
      count(1) as unique_key_tgt_loaded
    from (
      select 
        sg.county_name , sg.time_period_date_end , sg.rv_pagov__opioid_stays_hd 
      from 
        dw_dev.dbt_steve.stg_pagov__opioid_stays as sg
      where exists 
      (
        select 
          1
        from dw_dev.dbt_steve.rv_pagov__opioid_stays as tg
        where 
          sg.rv_pagov__opioid_stays_job_id = tg.rv_pagov__opioid_stays_job_id and
          nvl(sg.county_name::varchar, '') = nvl(tg.county_name::varchar, '') and nvl(sg.time_period_date_end::varchar, '') = nvl(tg.time_period_date_end::varchar, '') and nvl(sg.rv_pagov__opioid_stays_hd::varchar, '') = nvl(tg.rv_pagov__opioid_stays_hd::varchar, '')  
      )
      group by
        sg.county_name , sg.time_period_date_end , sg.rv_pagov__opioid_stays_hd 
    )
),
FETCH_SAT_STATS3_stg_pagov__opioid_stays
as (
  select 
    count(1) unique_key_tgt_total
  from dw_dev.dbt_steve.rv_pagov__opioid_stays
 )
 select 
  '10828c61-cb2d-4d4b-8dd3-2a13bac161f7' as job_id,
  'stg_pagov__opioid_stays' as stg_table_name,
  'rv_pagov__opioid_stays' as tgt_table_name,
  unique_key_stg_columns,
  unique_key_tgt_columns,
  unique_key_stg_gross,
  unique_key_stg_unique,
  unique_key_tgt_not_loaded,
  unique_key_tgt_loaded,
  unique_key_tgt_total,
  to_char(timestamp '2023-05-07 19:59:41.933416+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp  as loaded_at,
  to_char(sysdate(), 'YYYY-MM-DD HH24:MI:SS')::timestamp inserted_at
from 
  SAT_UKEY_SGTG_stg_pagov__opioid_stays, 
  FETCH_SAT_STATS1_stg_pagov__opioid_stays, 
  FETCH_SAT_STATS2_stg_pagov__opioid_stays, 
  FETCH_SAT_STATS3_stg_pagov__opioid_stays


            )

    

    







    
        
        select * 
        from test_pagov__opioid_stays
    
    
    






)select 
   * 
from __dbt__cte___stg_rv_recon_test