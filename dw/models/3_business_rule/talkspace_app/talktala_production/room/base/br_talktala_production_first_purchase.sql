{{
    config(
        tags=['hold']
    )
}}

with room_first_conversion as
(
    select
        private_talk_id as room_id,
        conversion_date as first_conv_date,
        conversion_therapist_id as first_conv_provider_id,
        conversion_transaction_uuid as first_conv_charge_id,
        conversion_transaction_subscription_uuid as first_conv_subscription_id,
        conversion_transaction_amount as first_conv_charge_amount,
        funnel_variation as first_conv_funnel_variation,
        payment_type as first_conv_payment_type_id,
        /*{{ ___TO_BE_DELETED('first_conv') }},*/
        {{ build_dbt_metadata_cols('first_conv') }}
    from {{ ref('rv_talktala_production_first_purchase') }}
)
, room as
(
    select room_id
    from {{ ref('br_talktala_production_private_talks') }}
    group by room_id
)
, final as
(
    select room_first_conversion.*
    from room_first_conversion
    join room
    on room_first_conversion.room_id = room.room_id
)
select *
from final