{{
    config(
        tags=['hold']
    )
}}

with plan as
(
    select
        *
    from
        {{ ref('br_plan') }}
    /*where true
        and plan_details_dbt_valid_to is null
        and plan_dbt_valid_to is null
        and plan_group_dbt_valid_to is null
        and plan_credit_settings_dbt_valid_to is null
        and auto_cancel_settings_dbt_valid_to is null*/
)
, partner as
(

    select
        * 
    from
        {{ ref('br_partner') }}
    where true
        --and partner_dbt_valid_to is null
)
, final as
(
    select
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
        plan.partner_id,
        partner.partner_name,
        plan.session_based_payout,
        plan.service_type,
        plan.account_type,
        plan.billing_format,
        plan.product_line,
        plan.plan_modality,
        plan.bundle_value,
        plan.duration_in_days,
        plan.net_therapist_earning_rate_per_bundled_video,
        plan.duration_in_minute,
        plan.customer_pay_total_rate_messaging,
        plan.net_therapist_earning_rate_with_all_bundled_video,
        plan.plan_group_id,
        plan.price_per_video_credit,
        plan.minutes_per_video_credit,
        plan.video_credits_per_batch,
        plan.batches_per_billing_cycle,
        plan.is_auto_cancellation_enabled,
        plan.auto_cancellation_period_unit,
        plan.auto_cancellation_period_value

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
        {{ ___TO_BE_DELETED('auto_cancel_settings') }},

        {{ ___TO_BE_DELETED('partner') }},
        {{ ___TO_BE_DELETED('partner') }}*/
    from
        plan
        left join partner
            on plan.partner_id = partner.partner_id
    where true
)
select
    *
from
    final
where true