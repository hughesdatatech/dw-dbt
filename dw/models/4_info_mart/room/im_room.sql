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
        {{ ref('im_plan') }}
    where true
)
, payment_type as 
(
    select
        *
    from
        {{ ref('im_payment_type') }}
    where true
)
, plan_and_payment_type as 
(
    select distinct
        payment_type.payment_type_id,
        payment_type.plan_id,
        plan.partner_id,
        plan.partner_name,
        plan.account_type
    from
        payment_type
        left join plan
            on payment_type.plan_id = plan.plan_id
    where true
)
, room as
(
    select
        *
    from
        {{ ref('br_room') }} 
   /* where true
        and room_dbt_valid_to is null
        and first_conv_dbt_valid_to is null
        and user_dbt_valid_to is null*/
)
, final as
(
    select
        /* room attributes */
        room.room_id,
        room.provider_id,
        room.client_id,
        room.spouse_client_id,
        room.room_type,
        room.room_status,
        room.created_at,
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
        room.has_converted,
        room.first_conv_date,
        room.first_conv_provider_id,
        room.first_conv_charge_id,
        room.first_conv_subscription_id,
        room.first_conv_charge_amount,
        room.first_conv_funnel_variation,
        room.first_conv_payment_type_id,

        /* plan and payment type attributes */
        room.payment_type_id,
        plan_and_payment_type.plan_id,
        plan_and_payment_type.partner_id,
        plan_and_payment_type.partner_name,
        plan_and_payment_type.account_type,

        plan_and_payment_type_fc.plan_id as first_conv_plan_id,
        plan_and_payment_type_fc.partner_id as first_conv_partner_id,
        plan_and_payment_type_fc.account_type as first_conv_account_type

        /* meta fields */
        /*
        {{ ___TO_BE_DELETED('room') }},
        {{ ___TO_BE_DELETED('room') }},

        {{ ___TO_BE_DELETED('first_conv') }},
        {{ ___TO_BE_DELETED('first_conv') }}*/
    from
        room
        left join plan_and_payment_type
            on room.payment_type_id = plan_and_payment_type.payment_type_id
        left join plan_and_payment_type as plan_and_payment_type_fc
            on room.first_conv_payment_type_id = plan_and_payment_type_fc.payment_type_id
    where true
)
select
    *
from
    final
where true