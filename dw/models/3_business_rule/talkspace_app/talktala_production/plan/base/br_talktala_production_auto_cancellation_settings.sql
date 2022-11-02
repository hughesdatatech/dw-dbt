{{
    config(
        tags=['hold']
    )
}}

with final as
(
    select
        id as auto_cancellation_id,
        plan_id,
        enabled as is_auto_cancellation_enabled,
        period_unit as auto_cancellation_period_unit,
        period_value as auto_cancellation_period_value,
        created_at,
        updated_at,
        /*{{ ___TO_BE_DELETED('auto_cancel_settings') }},*/
        {{ build_dbt_metadata_cols('auto_cancel_settings') }}
    from {{ ref('rv_talktala_production_auto_cancellation_settings') }}
)
select *
from final