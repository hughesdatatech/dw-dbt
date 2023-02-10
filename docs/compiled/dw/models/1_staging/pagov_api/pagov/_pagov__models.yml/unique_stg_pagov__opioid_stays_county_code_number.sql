
    
    

select
    county_code_number as unique_field,
    count(*) as n_records

from dw_dev.dbt_steve.stg_pagov__opioid_stays
where county_code_number is not null
group by county_code_number
having count(*) > 1


