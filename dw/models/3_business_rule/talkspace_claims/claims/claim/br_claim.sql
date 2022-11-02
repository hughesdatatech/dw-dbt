{% set jira_task_key = 'PLATFORM-1922' %}

with 

sess_rep as (

    select
        im_session_report_hk,
        im_session_report_metadata,
        rv_talktala_production__session_reports_metadata,
        session_report_id as _session_report_id,
        therapy_duration_minutes,
        completed_at,
        is_complete
    from {{ ref('im_session_report') }}

),

final as (

    select
        claim.*,
        sess_rep.im_session_report_hk,
        sess_rep.im_session_report_metadata,
        sess_rep.rv_talktala_production__session_reports_metadata,
        sess_rep.therapy_duration_minutes,
        sess_rep.completed_at,
        case
            when claim.is_test_claim <> 1 
                and claim.claim_status <> 'cancelled' 
                and claim.rv_claims__claims_key_status = 'active'
                and sess_rep.is_complete
                then True
            else False
        end is_valid_and_complete
    from {{ ref('br_claim_version_1794') }} claim
    left join sess_rep on claim.session_report_id = sess_rep._session_report_id

    union all

    select
        claim.*,
        sess_rep.im_session_report_hk,
        sess_rep.im_session_report_metadata,
        sess_rep.rv_talktala_production__session_reports_metadata,
        sess_rep.therapy_duration_minutes,
        sess_rep.completed_at,
        case
            when 
                claim.is_test_claim <> 1 
                and claim.claim_status <> 'cancelled' 
                and claim.rv_claims__claims_key_status = 'active'
                and sess_rep.is_complete
            then True
            else False
        end is_valid_and_complete
    from {{ ref('br_claim_version_1922') }} claim
    left join sess_rep on claim.session_report_id = sess_rep._session_report_id

)

select
    '{{ jira_task_key }}' as jira_task_key,
     {{ 
        build_hash_value(
            value=build_hash_diff(
                        cols=['br_claim_version_jira_task_key', 'br_claim_version_hk', 'im_session_report_hk']
                    ),
            alias='br_claim_hk'
        )
    }},
    *
from final
