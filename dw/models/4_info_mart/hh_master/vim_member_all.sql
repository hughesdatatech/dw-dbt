{{ config(materialized='view') }}

with

all_member as (

    select
        *,
        'N/A' as exception_list
    from 
        {{ ref('im_member') }}
    where true
    
),

exception_member as (

    select
       *
    from 
        {{ ref('im_member_exception') }}
    where true


),

final as (

    select 
       *
    from all_member
    where true
        
    union all        

    select
        *
    from exception_member
    where true

)

select *
from final
where true
