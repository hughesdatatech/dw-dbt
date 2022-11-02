{{
    config(
        tags=['hold']
    )
}}

with final as 
(
    select
        *
    from
        {{ ref('br_partner') }}
    where true
        --and partner_dbt_valid_to is null
)
select
    *
from
    final
where true