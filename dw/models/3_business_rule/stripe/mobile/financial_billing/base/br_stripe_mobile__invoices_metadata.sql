{% set alias = this.name.replace('br_', '') %}

with 

rv_base as (

    select
         -- business key
        invoice_id,
        "key" as metadata_key,
        
        -- fks
        merchant_id,

        -- misc
        {{ coalesce_column(column='"value"', alias='metadata_value') }},

        -- metrics

        -- dates

        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    where true

),

{{ build_br_base_model(alias) }}
