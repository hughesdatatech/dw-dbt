with

all_member as (

    select
        *
    from 
        {{ ref('br_member') }}
    where true
    
),

exception_member as (

    select
       *
    from 
        {{ ref('br_member_exception') }}
    where true


),

final as (

    select 
        all_member.* 
    from 
        all_member
        left join exception_member
            on all_member.member_hk = exception_member.member_hk
    where true
        and exception_member.member_hk is null

)

select *
from final
where true
