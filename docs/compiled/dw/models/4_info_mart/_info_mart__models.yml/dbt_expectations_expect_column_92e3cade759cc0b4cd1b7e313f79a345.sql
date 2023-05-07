

with all_values as (

    select
        company_name as value_field

    from dw_dev.dbt_steve.vim_member_all
    

),
set_values as (

    select
        cast('Unknown' as TEXT) as value_field
    
    
),
validation_errors as (
    -- values from the model that match the set
    select
        v.value_field
    from
        all_values v
        join
        set_values s on v.value_field = s.value_field

)

select *
from validation_errors

