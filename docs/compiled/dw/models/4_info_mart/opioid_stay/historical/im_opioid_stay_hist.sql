

select
    opioid_stay.*
from 
    dw_dev.dbt_steve.im_opioid_stay as opioid_stay

    left join dw_dev.dbt_steve.im_opioid_stay_hist opioid_stay_hist on 
        opioid_stay.im_opioid_stay_hk = opioid_stay_hist.im_opioid_stay_hk
where true
    and opioid_stay_hist.im_opioid_stay_hk is null
