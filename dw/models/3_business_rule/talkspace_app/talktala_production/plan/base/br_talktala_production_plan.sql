{{
    config(
        tags=['hold']
    )
}}

with final as
(
    select
        plan_id,
        plan_type as plan_name,
        plan_sessions,
        credit_minutes_options,
        credit_minutes,
        video_modality::boolean as is_video_modality,
        audio_modality::boolean as is_audio_modality,
        chat_modality::boolean as is_chat_modality,
        internal_plan_name,
        plan_display_json,
        billing_charge_price_usd as billing_charge_price,
        billing_cycle_unit,
        billing_cycle_value,
        billing_auto_renew,
        billing_stripe_plan_id,
        billing_recurly_plan_id,
        free_trial_days,
        claim_type,
        plan_message_text,
        currency,
        gedi_payer_id,
        voucher_auto_renew,
        /*{{ ___TO_BE_DELETED('plan') }},*/
        {{ build_dbt_metadata_cols('plan') }}
    from {{ ref('rv_talktala_production_plan') }}
)
select *
from final
