{% set alias = this.name.replace('br_', '') %}
with 

rv_base as (

    select
         -- business key
        id as subscription_id,
    
        -- fks
        customer_id,
        {{ user_id_from_customer_id('customer_id') }},
        default_source_id,
        discount_coupon_id,
        discount_customer_id,
        discount_subscription,
        merchant_id,
        plan_id,
        price_id,

        -- misc
        billing,
        billing_thresholds_reset_billing_cycle_anchor as is_billing_thresholds_reset_billing_cycle_anchor,
        cancel_at_period_end as will_cancel_at_period_end,
        cancellation_reason,
        cancellation_reason_text,
        pause_collection_behavior,
        "status" as subscription_status,

        -- metrics
        application_fee_percent,
        billing_thresholds_amount_gte,
        days_until_due,
        quantity,
        tax_percent,
        
        -- dates
        billing_cycle_anchor as billing_cycle_anchored_at,
        cancel_at,
        canceled_at,
        created as created_at,
        current_period_end as current_period_ended_at,
        current_period_start as current_period_started_at,
        discount_end as discount_ended_at,
        discount_start as discount_started_at,
        ended_at as ended_at,
        pause_collection_resumes_at as pause_collection_resumed_at,
        start_time as started_time_at,
        "start_date" as started_at,
        trial_end as trial_ended_at,
        trial_start as trial_started_at,

        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    where true

),

{{ build_br_base_model(alias) }}
