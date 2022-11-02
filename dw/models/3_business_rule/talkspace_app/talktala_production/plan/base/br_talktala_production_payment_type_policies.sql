{{
    config(
        tags=['hold']
    )
}}

with final as
(
    select
        id as payment_type_policy_id,
        payment_type_id,
        includes_psychiatry,
        includes_dependents,
        minimum_age,
        crisis_protocol,
        additional_plan_information,
        /*{{ ___TO_BE_DELETED('payment_type_policies') }},*/
        {{ build_dbt_metadata_cols('payment_type_policies') }}
   from {{ ref('rv_talktala_production_payment_type_policies') }}
)
select *
from final