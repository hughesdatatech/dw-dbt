{{
    config(
        tags=['hold']
    )
}}

with users as
(
    select
        user_id/*,
        {{ ___TO_BE_DELETED('user') }},
        {{ ___TO_BE_DELETED('user') }}*/
    from
        {{ ref('br_talktala_production_users') }}
    where true
        and is_test_user = 0
)
,room as
(
	select
        id as room_id,
        therapist_id as provider_id,
        participant_id as client_id,
        spouse_id as spouse_client_id,
        room_type,
        status as room_status,
        created_at,
        payment_type as payment_type_id,
        bought_from,
        last_message_dashboard_id,
        last_participant_message_id,
        funnel_variation,
        therapy_weekday,
        spawned_from_private_talk as spawned_from_room,
        conversion_therapist_id as conv_provider_id,
        allow_view_messages_before_therapist_joined_room as allow_view_messages_before_provider_joined_room,
        allow_view_messages_confirmation_date,
        trial_end,
        reminder_count_updated_at,
        {{ ___TO_BE_DELETED('room') }},
        {{ build_dbt_metadata_cols('room') }}
	from
        {{ ref('rv_talktala_production_private_talks') }}
    where true
)
,final as
(
    select
        room.*,
        {{ ___TO_BE_DELETED('user') }},
        {{ ___TO_BE_DELETED('user') }}
    from
        room
        join users
            on room.client_id = users.user_id
    where true
)
select
    *
from
    final
where true
