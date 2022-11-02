{% set alias = this.name.replace('br_', '') %}

with 

rv_base as (

    select 
        -- business key
        id as transaction_id,

        -- fks
        claim_id,
        invoice_id,
        charge_id,
        refund_id,

        -- misc
        
        -- dates
        created_at,
        
        -- metrics
        {{ decimalize(column='amount', alias='transaction_amount') }},
        
        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    where true

),

{{ build_br_base_model(alias) }}
