



select
    *,
    im_opioid_stay_loaded_at as point_in_time_at,
    convert_timezone('UTC', 'America/New_York',
    cast(im_opioid_stay_loaded_at as TIMESTAMP)
) as point_in_time_at_et,
    False as is_point_in_time
from 
    dw_dev.dbt_steve.im_opioid_stay