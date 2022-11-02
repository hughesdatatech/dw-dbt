{{
    config(
        tags=['hold']
    )
}}

with final as
(
    select
        plan_type_id as plan_id,
        private_talks_payment_type_id as payment_type_id,
        /*{{ ___TO_BE_DELETED('payment_type_plan') }},*/
        {{ build_dbt_metadata_cols('payment_type_plan') }}
    from {{ ref('rv_talktala_production_plan_type_payment_type_mapping') }}
)
select *
from final