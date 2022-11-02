{% set alias = this.name.replace('br_', '') %}

with 

rv_base as (
    
    select
        -- business key
        id as refund_id,

        -- fks
        balance_transaction_id,
        charge_id,
        merchant_id,

        -- misc
        acquirer_reference_number,
        currency,
        failure_reason,
        reason,
        receipt_number,
        "status" as refund_status,
        
        -- metrics
        {{ decimalize(column='amount') }},

        -- dates
        created as created_at,
    
        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    where true

),

{{ build_br_base_model(alias) }}
