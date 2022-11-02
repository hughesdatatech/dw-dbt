{{
    config(
        tags=['hold']
    )
}}

with final as
(
    select
        id as plan_credit_settings_id,
        plan_id,
        price as price_per_video_credit,
        minutes as minutes_per_video_credit,
        quantity as video_credits_per_batch,
        batches as batches_per_billing_cycle,
        updated_at,
        /*{{ ___TO_BE_DELETED('plan_credit_settings') }},*/
        {{ build_dbt_metadata_cols('plan_credit_settings') }}
    from {{ ref('rv_talktala_production_plan_credit_settings') }}
)
select *
from final