{{
    config(
        tags=['hold']
    )
}}

with payment_type as
(
    select *
    from {{ ref('br_talktala_production_payment_types') }}
)
, plan_type_payment_type_mapping as
(
    select *
    from {{ ref('br_talktala_production_plan_type_payment_type_mapping') }}
)
, payment_type_policies as
(
    select *
    from {{ ref('br_talktala_production_payment_type_policies') }}
)

, final as
(
    select distinct
        /* payment_type attributes */
        payment_type.payment_type_id,
        payment_type.payment_type_name,
        payment_type.google_tag_manager_name,
        payment_type.default_freeze_period,
        payment_type.provider,
        payment_type.client_display_variant,
        payment_type.provider_display_variant,
        payment_type.admin_display_variant,

        /* plan_type_payment_type attributes */
        plan_type_payment_type_mapping.plan_id,

        /* payment_type_policies attributes */
        payment_type_policies.payment_type_policy_id,
        payment_type_policies.includes_psychiatry,
        payment_type_policies.includes_dependents,
        payment_type_policies.minimum_age,
        payment_type_policies.crisis_protocol,
        payment_type_policies.additional_plan_information

        /* meta fields */
        /*
        {{ ___TO_BE_DELETED('payment_types') }},
        {{ ___TO_BE_DELETED('payment_types') }},

        {{ ___TO_BE_DELETED('payment_type_plan') }},
        {{ ___TO_BE_DELETED('payment_type_plan') }},

        {{ ___TO_BE_DELETED('payment_type_policies') }},
        {{ ___TO_BE_DELETED('payment_type_policies') }}*/
    from payment_type
    left join plan_type_payment_type_mapping
    on payment_type.payment_type_id = plan_type_payment_type_mapping.payment_type_id
    left join payment_type_policies
    on payment_type.payment_type_id = payment_type_policies.payment_type_id
)
select *
from final