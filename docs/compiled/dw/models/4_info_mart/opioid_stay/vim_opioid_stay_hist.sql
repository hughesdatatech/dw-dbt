with

hist as (

    select
        *,
        im_opioid_stay_loaded_at as im_opioid_stay_effective_starting_at,
        coalesce(
            lead(im_opioid_stay_loaded_at) over (
                partition by rv_pagov__opioid_stays_hk
                order by im_opioid_stay_loaded_at
            ),
            '9999-12-31 00:00:00'
        ) as im_opioid_stay_effective_ending_at
    from dw_dev.dbt_steve.im_opioid_stay_hist

)

select
    *,
    case
        when im_opioid_stay_effective_ending_at = '9999-12-31 00:00:00' then true
        else false
    end as is_current_record
from hist

