{% set alias = this.name.replace('br_', '') %}

with 

rv_base as (

    select 
        -- business key
        id as era_id,

        -- fks
        {{ coalesce_column(column='payer_name', replace_with=var("null_key_char")) }},

        -- misc
        {{ coalesce_column(column='raw_835_storage_key') }},
        {{ coalesce_column(column='payment_method', alias='era_payment_method') }},
        {{ coalesce_column(column='check_id', alias='era_check_id') }},
        {{ coalesce_column(column='era_response') }},
        {{ coalesce_column(column='has_been_paid', replace_with=0, alias='is_paid_era') }},
        {{ coalesce_column(column='is_manual', replace_with=0, alias='is_manual_era') }},

        -- dates
        created_at,
        received_at,
        check_date as check_dated_at,
        
        -- metrics
        {{ decimalize(column='check_amount', alias='era_check_amount') }},
        claim_count as era_claim_count,

        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    where true

),

{{ build_br_base_model(alias) }}
