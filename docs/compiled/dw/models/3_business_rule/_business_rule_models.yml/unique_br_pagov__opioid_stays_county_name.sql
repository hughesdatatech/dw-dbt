
    
    

select
    county_name as unique_field,
    count(*) as n_records

from dw_dev.dbt_steve.br_pagov__opioid_stays
where county_name is not null
group by county_name
having count(*) > 1


