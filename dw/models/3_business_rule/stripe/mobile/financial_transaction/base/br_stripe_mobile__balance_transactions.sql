{% set alias = this.name.replace('br_', '') %}

with 

rv_base as (
   
    select
        -- business key
        id as balance_transaction_id,

        -- fks
        automatic_transfer_id,
        merchant_id,
        source_id,

        -- misc
        currency,
        "description" as balance_transaction_description,
        reporting_category,
        "status" as balance_transaction_status,
        "type" as balance_transaction_type,

        -- metrics
        {{ decimalize(column='amount', alias='gross_amount') }},
        {{ decimalize(column='fee', alias='fee_amount') }},
        {{ decimalize(column='net', alias='net_amount') }},

        -- dates
        available_on as available_at,
        created as created_at,
   
        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    where true

),

{{ build_br_base_model(alias) }}
