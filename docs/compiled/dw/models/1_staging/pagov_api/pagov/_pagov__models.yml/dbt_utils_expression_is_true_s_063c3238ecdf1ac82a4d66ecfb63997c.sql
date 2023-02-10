



select
    1
from dw_dev.dbt_steve.stg_pagov__opioid_stays

where not(rate_of_maternal_stays_with < 50)

