{% set alias = this.name.replace('br_', '') %}

with 

rv_base as (
    
    select
        -- business key
        id as dispute_id,

        -- fks
        charge_id,
        evidence_cancellation_policy_id,
        evidence_customer_communication_id,
        evidence_customer_signature_id,
        evidence_duplicate_charge_documentation_id,
        evidence_duplicate_charge_id,
        evidence_receipt_id,
        evidence_refund_policy_id,
        evidence_service_documentation_id,
        evidence_shipping_documentation_id,
        evidence_uncategorized_file_id,
        merchant_id,

        -- misc
        currency,
        evidence_cancellation_policy_disclosure,
        evidence_cancellation_rebuttal,
        evidence_details_has_evidence as has_evidence,
        evidence_details_past_due as is_past_due,
        evidence_details_submission_count,
        evidence_product_description,
        evidence_refund_policy_disclosure,
        evidence_refund_refusal_explanation,
        evidence_service_date,
        evidence_shipping_carrier,
        evidence_shipping_date,
        evidence_uncategorized_text,
        is_charge_refundable,
        network_details_type,
        network_details_visa_rapid_dispute_resolution as is_rapid_dispute_resolution,
        network_reason_code,
        reason,
        "status" as dispute_status,
        
        -- metrics
        {{ decimalize(column='amount') }},
        
        -- dates
        created as created_at,
        evidence_details_due_by as evidence_due_at,

        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    where true

),

{{ build_br_base_model(alias) }}
