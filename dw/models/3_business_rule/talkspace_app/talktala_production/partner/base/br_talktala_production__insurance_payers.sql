{% set alias = this.name.replace('br_', '') %}

with 

rv_base as (

        select
            -- business keys
        id as insurance_payer_id,

        -- fks
        {{ coalesce_column(column='payer_id', replace_with=var("null_key_char"), alias='payer_id') }},

        -- misc
        {{ coalesce_column(column='label', alias='payer_name') }},
        {{ coalesce_column(column='sanitized_keyword') }},
        {{ coalesce_column(column='is_primary', replace_with=0, alias='is_primary_payer') }},

        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from 
        {{ ref('rv_' + alias) }} as rv
    where true

),

{{ build_br_base_model(alias) }}
