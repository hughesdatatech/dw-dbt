{{
    config(
        tags=['hold']
    )
}}

with final as
(
    select
        id as payment_type_id,
        name as payment_type_name,
        google_tag_manager_name,
        default_freeze_period,
        provider,
        customer_display_variant as client_display_variant,
        therapist_display_variant as provider_display_variant,
        admin_display_variant,
        /*{{ ___TO_BE_DELETED('payment_types') }},*/
        {{ build_dbt_metadata_cols('payment_types') }}
   from {{ ref('rv_talktala_production_payment_types') }}
)
select *
from final