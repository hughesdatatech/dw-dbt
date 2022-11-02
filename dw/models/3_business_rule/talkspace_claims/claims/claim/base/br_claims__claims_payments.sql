{% set alias = this.name.replace('br_', '') %}

with 

cp_prep1 as (

    select
        -- business key
        id as claim_payment_id,

        -- fks
        {{ coalesce_column(column='era_id', replace_with=var("null_key_int")) }},
        {{ coalesce_column(column='claim_id', replace_with=var("null_key_int")) }},
        {{ coalesce_column(column='prepaid_charge_id', replace_with=var("null_key_char")) }},
        {{ coalesce_column(column='balance_invoice_id', replace_with=var("null_key_char")) }},
        {{ coalesce_column(column='balance_charge_id', replace_with=var("null_key_char")) }},
        {{ coalesce_column(column='balance_refund_id', replace_with=var("null_key_char")) }},

        -- misc
        {{ coalesce_column(column='currency') }},
        {{ coalesce_column(column='adjustments_reason') }},
        {{ coalesce_column(column='invoice_payment_page_url') }},
        {{ coalesce_column(column='balance_status') }},
        {{ coalesce_column(column='cx_ticket_url') }},

        -- metrics
        {{ decimalize(column='cost_of_service') }},
        {{ decimalize(column='member_prepaid', alias='client_prepaid') }},
        {{ decimalize(column='deductible_estimated', alias='estimated_deductible') }},
        {{ decimalize(column='coinsurance_estimated', alias='estimated_coinsurance') }},
        {{ decimalize(column='copay_estimated', alias='estimated_copay') }},
        {{ decimalize(column='allowed_amount') }},
        {{ decimalize(column='deductible_final', alias='final_deductible') }},
        {{ decimalize(column='coinsurance_final', alias='final_coinsurance') }},
        {{ decimalize(column='copay_final', alias='final_copay') }},
        {{ decimalize(column='late_filing_fee') }},
        {{ decimalize(column='other_adjustments') }},
        {{ decimalize(column='insurance_paid') }},
        {{ decimalize(column='customer_balance') }},
        {{ decimalize(column='total_member_liability', alias='total_client_liability') }},
        {{ decimalize(column='claim_balance') }},
        {{ decimalize(column='admin_charge_amount') }},
        {{ decimalize(column='open_member_balance', alias='open_client_balance') }},
        {{ decimalize(column='write_off_amount') }},
        
        -- dates
        created_at,
        updated_at,
       
        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    where true

),

cp_prep2 as (

    select
        *,
        -- calculated metrics
        case
            when balance_status = 'paid' and allowed_amount - client_prepaid - insurance_paid > 0.00
            then allowed_amount - client_prepaid - insurance_paid
            else 0.00 
        end as client_postpaid
    from cp_prep1
),

rv_base as (
    
    select
        *,
        -- calculated metrics
        client_prepaid + client_postpaid + insurance_paid as total_paid,
        client_prepaid + client_postpaid as client_paid,
        case
            when balance_status = 'paid' and cost_of_service - (client_prepaid + client_postpaid) - insurance_paid < 0.00
            then cost_of_service - (client_prepaid + client_postpaid) - insurance_paid
            when cost_of_service - client_prepaid - insurance_paid < 0.00
            then cost_of_service - client_prepaid - insurance_paid
            else 0.00
        end as cms_refunds,
        cost_of_service - allowed_amount as cost_of_service_less_allowed_amount
    from cp_prep2
    where true

),

{{ build_br_base_model(alias) }}
