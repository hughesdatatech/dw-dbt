

select
    'PLATFORM-DEFAULT' as jira_task_key,
    br_pagov__opioid_stays_hk as br_opioid_stay_hk,
    
from 
    dw_dev.dbt_steve.br_pagov__opioid_stays
where true
    and br_pagov__opioid_stays_row_sqn_desc = 1