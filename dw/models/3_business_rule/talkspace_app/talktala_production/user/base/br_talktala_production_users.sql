{{
    config(
        tags=['hold']
    )
}}

with final as
(
    select
        id as user_id,
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
        partner_id as first_plan_partner_id,
        payfirst_account_completed_at,
        payfirst_account_not_completed_warning_sent_at,
        activation_code_created_at,
        email_verification_status,
        email_verification_date,
        case
            when 
            {%- for email in var('test_user_emails') %} email ilike '%{{ email }}%'{% if not loop.last %} or {%- endif %}{% endfor %}
            then True 
            else False
        end as is_test_user,
        /*{{ ___TO_BE_DELETED('user') }},*/
        {{ build_dbt_metadata_cols('user') }}
    from {{ ref('rv_talktala_production_users') }}
)
select *
from final