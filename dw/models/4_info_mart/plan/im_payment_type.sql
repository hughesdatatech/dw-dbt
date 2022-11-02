{{
    config(
        tags=['hold']
    )
}}

with final as 
(
    select * 
    from {{ ref('br_payment_type') }}
    --where payment_types_dbt_valid_to is null
    --and payment_type_plan_dbt_valid_to is null
    --and payment_type_policies_dbt_valid_to is null
)
select *
from final