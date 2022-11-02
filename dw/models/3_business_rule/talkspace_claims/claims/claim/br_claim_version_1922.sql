{% set jira_task_key = 'PLATFORM-1922' %}

select
    -- metadata
    '{{ jira_task_key }}' as br_claim_version_jira_task_key, -- unique to claims since we are mantaining two versions at the same time
     {{ 
        build_hash_value(
            value=build_hash_diff(
                        cols=[
                                'br_claims__claims_hk', 
                                'br_claims__claims_data_hk', 
                                'br_claims__claims_submitted_hk',
                                'br_claims__claims_payments_hk',
                                'br_claims__claims_eras_hk'
                        ]
                    ),
            alias='br_claim_version_hk'
        )
    }},
    br_claims__claims_hk,
    br_claims__claims_data_hk,
    br_claims__claims_submitted_hk,
    br_claims__claims_payments_hk,
    br_claims__claims_eras_hk,
    
    -- business keys
    claim.claim_id,
    claim_data.claim_data_id,
    claim_submission.claim_submission_id,
    claim_payment.claim_payment_id,
    claim_era.era_id,
    claim.session_report_id,
    
    -- fks to other key data not yet being related to
    claim.client_id,
    claim.room_id,
    claim.provider_id,
    claim.admin_id,

    -- misc fields - claim
    claim.claim_status,
    claim.claim_status_reason,
    claim.notes,
    claim.is_test_claim,
    claim.is_escalated_claim,

    -- misc fields - claim_data
    claim_data.payer_id, -- relates to seed data in ref_payer_names.csv
    claim_data.payer_name_manual,
    claim_data.payer,
    claim_data.benefit_type,
    claim_data.service_modality,
    claim_data.diagnosis_code,
    claim_data.diagnosis_name,
    claim_data.service_cpt_code,
    claim_data.service_name,

    -- misc fields - claim_submission
    -- TBD

    -- misc fields - claim_payment
    claim_payment.balance_status,
    claim_payment.claim_balance,
    claim_payment.admin_charge_amount,

    -- misc fields - claim_era
    claim_era.is_paid_era,

    -- dates
    claim.created_at,
    claim_data.service_started_at,
    claim_data.service_ended_at,
    claim_data.report_submitted_at,

    -- calculated columns
    -- Cost of Service, same as 1794
    coalesce(claim_payment.cost_of_service, 0.00) as cost_of_service,
    -- Expected Revenue, updated for 1922
    case
        when claim.claim_status = 'complete'
        then coalesce(claim_charge_prepay.charge_amount, 0.00) + coalesce(claim_charge_postpay.charge_amount, 0.00) + coalesce(claim_payment.insurance_paid, 0.00)
        when claim_era.era_id is not null
        then coalesce(claim_payment.allowed_amount, 0.00)
        else coalesce(claim_payment.cost_of_service, 0.00)
    end as expected_revenue, 
    -- Total Receivables Approved, updated for 1922
    coalesce(claim_payment.insurance_paid, 0.00) + coalesce(claim_charge_prepay.charge_amount, 0.00) + coalesce(claim_charge_postpay.charge_amount, 0.00) as total_paid,
    -- Insurance Payments per ERA, same as 1794
    coalesce(claim_payment.insurance_paid, 0.00) as insurance_paid, 
    -- Insurance Payments in Bank, same as 1794
    coalesce( 
        case 
            when claim_era.is_paid_era = 1
            then claim_payment.insurance_paid
            else 0.00
        end
    , 0.00) as insurance_payments_in_bank,
    -- Insurance Owes Per ERA (not in the bank), updated for 1922
    coalesce( 
        case 
            when claim_era.is_paid_era = 0
            then claim_payment.insurance_paid
            else 0.00
        end
    , 0.00) as insurance_payments_owed_post_era,
    -- Insurance Owes Pending ERA, updated for 1922
    coalesce( 
        case 
            when claim_era.era_id is null
            then claim_payment.cost_of_service
            else 0.00
        end
    , 0.00) as insurance_owes,
    -- Total Insurance Owes, updated for 1922
    coalesce(
        case 
            when claim_era.era_id is null
            then claim_payment.cost_of_service
            when claim_era.is_paid_era = 0
            then claim_payment.insurance_paid
            else 0.00 
        end
    , 0.00) as total_insurance_owes, 
    -- Client Paid, updated for 1922
    coalesce(claim_charge_prepay.charge_amount, 0.00) + coalesce(claim_charge_postpay.charge_amount, 0.00) as client_paid,
    -- Client Prepaid, updated for 1922
    coalesce(claim_charge_prepay.charge_amount, 0.00) as client_prepaid,
    -- Client Postpaid, updated for 1922
    coalesce(claim_charge_postpay.charge_amount, 0.00) as client_postpaid,
    -- CMS Driven Client Refunds, updated for 1922
    coalesce(claim_refund.refund_amount, 0.00) as cms_refunds,
    -- Client Owes, updated for 1922
    coalesce(claim_payment.open_client_balance, 0.00) as client_owes,
    -- Remaining Balance, updated for 1922
    coalesce(claim_payment.claim_balance, 0.00) as remaining_balance,
    -- Expected Write Off, n/a for 1922
    NULL::numeric as expected_write_off,
    -- Cost of Service Less Allowed Amount, same as 1794
    coalesce(claim_payment.cost_of_service_less_allowed_amount, 0.00) as cost_of_service_less_allowed_amount,
    -- Allowed Amount, same as 1794
    coalesce(claim_payment.allowed_amount, 0.00) as allowed_amount,
    -- Write Off Amount, same as 1794
    coalesce(claim_payment.write_off_amount, 0.00) as write_off_amount,

    -- metadata
    rv_claims__claims_key_status,
    rv_claims__claims_metadata,
    rv_claims__claims_data_metadata,
    rv_claims__claims_submitted_metadata,
    rv_claims__claims_payments_metadata,
    rv_claims__claims_eras_metadata
from {{ ref('br_claims__claims') }} as claim
left join {{ ref('br_claims__claims_data') }} as claim_data 
    on claim.claim_data_id = claim_data.claim_data_id 
    and br_claims__claims_data_row_sqn_desc = 1
left join {{ ref('br_claims__claims_submitted') }} as claim_submission 
    on claim.claim_submission_id = claim_submission.claim_submission_id 
    and br_claims__claims_submitted_row_sqn_desc = 1
left join {{ ref('br_claims__claims_payments') }} as claim_payment 
    on claim.claim_payment_id = claim_payment.claim_payment_id 
    and br_claims__claims_payments_row_sqn_desc = 1
left join {{ ref('br_claims__claims_eras') }} as claim_era 
    on claim_era.era_id = claim_submission.era_id 
    and br_claims__claims_eras_row_sqn_desc = 1
left join {{ ref('br_claim_charge_agg')}} as claim_charge_prepay 
    on claim.claim_id = claim_charge_prepay.claim_id 
    and claim_charge_prepay.is_prepaid_charge = True
left join {{ ref('br_claim_charge_agg')}} as claim_charge_postpay 
    on claim.claim_id = claim_charge_postpay.claim_id 
    and claim_charge_postpay.is_prepaid_charge = False
left join {{ ref('br_claim_refund_agg')}} as claim_refund 
    on claim.claim_id = claim_refund.claim_id
where
    br_claims__claims_row_sqn_desc = 1
