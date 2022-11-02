{{
    config(
        tags=['hold']
    )
}}

with final as
(
    select
        plan_id,
        partner_id,
        session_based_payout,
        service_type,
        account_type,
        billing_format,
        product_line,
        plan_modality,
        customer_total_rate,
        bundle_value,
        duration_in_days,
        net_therapist_earning_rate_per_bundled_video,
        duration_in_minute,
        customer_pay_total_rate_messaging,
        net_therapist_earning_rate_with_all_bundled_video,
        created_at,
        updated_at,
        /*{{ ___TO_BE_DELETED('plan_details') }},*/
        {{ build_dbt_metadata_cols('plan_details') }}
    from {{ ref('rv_talktala_production_plan_details') }}
)
select *
from final