

with



    

        test_pagov__opioid_stays as (
                

with
SAT_KEY_DUPES_stg_pagov__opioid_stays as (
    select
        count(e) as key_dupes,
        'county_name,time_period_date_end,rv_pagov__opioid_stays_hd,rv_pagov__opioid_stays_loaded_at' as key_dupes_tgt_columns
    from (
        select
            count(*) as e
        from 
            dw_dev.dbt_steve.rv_pagov__opioid_stays
        group by
           
                county_name,
                time_period_date_end,
                rv_pagov__opioid_stays_hd,
                rv_pagov__opioid_stays_loaded_at
        having
            count(*) > 1
    )
)
select
    '056a736c-0a66-482a-8bea-9cf677869cab' as job_id,
    'stg_pagov__opioid_stays' as stg_table_name,
    'rv_pagov__opioid_stays' as tgt_table_name,
    key_dupes,
    key_dupes_tgt_columns,
    to_char(timestamp '2023-05-07 19:10:11.886586+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp  as loaded_at,
    to_char(sysdate(), 'YYYY-MM-DD HH24:MI:SS')::timestamp inserted_at
from 
    SAT_KEY_DUPES_stg_pagov__opioid_stays


            )

    

    







    
        
        select * 
        from test_pagov__opioid_stays
    
    
    





