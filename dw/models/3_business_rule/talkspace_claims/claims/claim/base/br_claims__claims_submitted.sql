{% set alias = this.name.replace('br_', '') %}

with 

rv_base as (

    select 
        -- business key
        id as claim_submission_id,

        -- fks
        {{ coalesce_column(column='claim_id', replace_with=var("null_key_int")) }},
        {{ coalesce_column(column='era_id', replace_with=var("null_key_int")) }},

        -- misc
        {{ coalesce_column(column='raw_837_storage_key') }},
        {{ coalesce_column(column='raw_999_storage_key') }},
        {{ coalesce_column(column='raw_277c_storage_key') }},
        {{ coalesce_column(column='raw_277p_storage_key') }},
        {{ coalesce_column(column='payer_control_number') }},
        {{ coalesce_column(column='external_id', alias='claim_submission_external_id') }},
        {{ coalesce_column(column='insurance_resolution') }},
        {{ coalesce_column(column='insurance_resolution_reason') }},
        {{ coalesce_column(column='insurance_resolution_details') }},
        {{ coalesce_column(column='submission_type', alias='claim_submission_type') }},

        -- dates
        submission_date as submitted_at,
        created_at,
        updated_at,

        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    where true

),

{{ build_br_base_model(alias) }}
