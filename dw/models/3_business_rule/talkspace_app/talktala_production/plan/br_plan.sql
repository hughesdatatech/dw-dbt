{{
    config(
        tags=['hold']
    )
}}

with plan_detail as
(
    select *
    from {{ ref('br_talktala_production_plan_details') }}
)
, plan_to_plan_group as
(
    select *
    from {{ ref('br_talktala_production_plan_to_plan_group') }}
)
, auto_cancellation_settings as
(
    select *
    from {{ ref('br_talktala_production_auto_cancellation_settings') }}
)
, plan_credit_settings as
(
    select *
    from {{ ref('br_talktala_production_plan_credit_settings') }}
)
, plan as
(
    select *
    from {{ ref('br_talktala_production_plan') }}
)
, final as
(
    select distinct
        /* plan attributes */
        plan.plan_id,
        plan.plan_name,
        plan.plan_sessions,
        plan.credit_minutes_options,
        plan.credit_minutes,
        plan.is_video_modality,
        plan.is_audio_modality,
        plan.is_chat_modality,
        plan.internal_plan_name,
        plan.plan_display_json,
        plan.billing_charge_price,
        plan.billing_cycle_unit,
        plan.billing_cycle_value,
        plan.billing_auto_renew,
        plan.billing_stripe_plan_id,
        plan.billing_recurly_plan_id,
        plan.free_trial_days,
        plan.claim_type,
        plan.plan_message_text,
        plan.currency,
        plan.gedi_payer_id,
        plan.voucher_auto_renew,

        /* plan detail attributes */
        plan_detail.partner_id,
        plan_detail.session_based_payout,
        plan_detail.service_type,
        plan_detail.account_type,
        plan_detail.billing_format,
        plan_detail.product_line,
        plan_detail.plan_modality,
        plan_detail.bundle_value,
        plan_detail.duration_in_days,
        plan_detail.net_therapist_earning_rate_per_bundled_video,
        plan_detail.duration_in_minute,
        plan_detail.customer_pay_total_rate_messaging,
        plan_detail.net_therapist_earning_rate_with_all_bundled_video,

        /* plan_to_plan_group attributes */
        plan_to_plan_group.plan_group_id,

        /* plan_credit_settings attributes */
        coalesce(plan_credit_settings.price_per_video_credit, 0) as price_per_video_credit,
        coalesce(plan_credit_settings.minutes_per_video_credit, 0) as minutes_per_video_credit,
        coalesce(plan_credit_settings.video_credits_per_batch, 0) as video_credits_per_batch,
        coalesce(plan_credit_settings.batches_per_billing_cycle, 0) as batches_per_billing_cycle,

        /* auto_cancellation_settings attributes */
        auto_cancellation_settings.is_auto_cancellation_enabled,
        auto_cancellation_settings.auto_cancellation_period_unit,
        auto_cancellation_settings.auto_cancellation_period_value

        /* meta fields */
        /*
        {{ ___TO_BE_DELETED('plan_details') }},
        {{ ___TO_BE_DELETED('plan_details') }},

        {{ ___TO_BE_DELETED('plan') }},
        {{ ___TO_BE_DELETED('plan') }},

        {{ ___TO_BE_DELETED('plan_group') }},
        {{ ___TO_BE_DELETED('plan_group') }},

        {{ ___TO_BE_DELETED('plan_credit_settings') }},
        {{ ___TO_BE_DELETED('plan_credit_settings') }},

        {{ ___TO_BE_DELETED('auto_cancel_settings') }},
        {{ ___TO_BE_DELETED('auto_cancel_settings') }}*/
    from plan
    left join plan_detail
    on plan.plan_id = plan_detail.plan_id
    left join plan_to_plan_group
    on plan.plan_id = plan_to_plan_group.plan_id
    left join plan_credit_settings
    on plan.plan_id = plan_credit_settings.plan_id
    left join auto_cancellation_settings
    on plan.plan_id = auto_cancellation_settings.plan_id
)
select *
from final