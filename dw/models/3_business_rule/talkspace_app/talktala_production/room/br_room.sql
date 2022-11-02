{{
    config(
        tags=['hold']
    )
}}

with room_first_conversion as
(
    select
        *
    from
        {{ ref('br_talktala_production_first_purchase') }}
    where true
)
, room as
(
    select
        *
    from
        {{ ref('br_talktala_production_private_talks') }}
    where true
)
, final as
(
    select
        room.room_id,
        room.provider_id,
        room.client_id,
        room.spouse_client_id,
        room.room_type,
        room.room_status,
        room.created_at,
        room.payment_type_id,
        room.bought_from,
        room.last_message_dashboard_id,
        room.last_participant_message_id,
        room.funnel_variation,
        room.therapy_weekday,
        room.spawned_from_room,
        room.conv_provider_id,
        room.allow_view_messages_before_provider_joined_room,
        room.allow_view_messages_confirmation_date,
        room.trial_end,
        room.reminder_count_updated_at,

        /* room first_conversion attributes */
        case
            when room_first_conversion.room_id is null then false
            else true
         end as has_converted,
        room_first_conversion.first_conv_date,
        room_first_conversion.first_conv_provider_id,
        room_first_conversion.first_conv_charge_id,
        room_first_conversion.first_conv_subscription_id,
        room_first_conversion.first_conv_charge_amount,
        room_first_conversion.first_conv_funnel_variation,
        room_first_conversion.first_conv_payment_type_id

        /* meta fields */
        /*
        {{ ___TO_BE_DELETED('room') }},
        {{ ___TO_BE_DELETED('room') }},

        {{ ___TO_BE_DELETED('first_conv') }},
        {{ ___TO_BE_DELETED('first_conv') }},

        {{ ___TO_BE_DELETED('user') }},
        {{ ___TO_BE_DELETED('user') }}*/
    from
        room
        left join room_first_conversion
            on room.room_id = room_first_conversion.room_id
    where true
)
select
    *
from
    final
where true