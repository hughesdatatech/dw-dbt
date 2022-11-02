{{
    config(
        tags=['hold']
    )
}}

with final as
(
    select *
    from {{ ref('br_client')}}
    --where user_dbt_valid_to is null
)
select *
from final