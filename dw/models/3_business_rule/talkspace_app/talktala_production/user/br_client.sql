{{
    config(
        tags=['hold']
    )
}}

with final as
(
	select
        user_id as client_id,
        timezone,
        user_type,
        created_at,
        last_login,
        status,
        deleted_at,
        newsletter_subscription,
        registered_from_domain,
        email_send_status,
        registration_full_url,
        funnel_variation_for_new_rooms,
        referred_by_user_id,
        registration_browser_referrer,
        first_plan_partner_id,
        payfirst_account_completed_at,
        payfirst_account_not_completed_warning_sent_at,
        activation_code_created_at,
        email_verification_status,
        email_verification_date/*,
        {{ ___TO_BE_DELETED('user') }},
        {{ ___TO_BE_DELETED('user') }}*/
	from {{ ref('br_talktala_production_users') }}
    where user_type=2
    and is_test_user is false
)
select *
from final