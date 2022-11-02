{% set alias = this.name.replace('br_', '') %}

with 

rv_base as (

    select 
        -- business key
        id as claim_data_id,

        -- fks
        {{ coalesce_column(column='claim_id', replace_with=var("null_key_int")) }},
        {{ coalesce_column(column='rv.payer_id', replace_with=var("null_key_char"), alias='payer_id') }},

        -- misc
        {{ coalesce_column(column='payer.payer_name', alias='payer_name_manual') }},
        {{ coalesce_column(column='payer.payer_name', replace_with='rv.payer_id', alias='payer') }},

        {{ coalesce_column(column='benefit_type') }},
        {{ coalesce_column(column='diagnosis_code') }},
        {{ coalesce_column(column='diagnosis_name') }},
        {{ coalesce_column(column='service_modality') }},

        {{ coalesce_column(column='member_id') }},
        {{ coalesce_column(column='member_authorization_code', alias='client_auth_code') }},
        auth_code_effective_date as client_auth_code_effective_at,
        {{ coalesce_column(column='member_first_name', alias='client_first_name') }},
        {{ coalesce_column(column='member_last_name', alias='client_last_name') }},
        {{ coalesce_column(column='member_group_number', alias='client_group_number') }},
        member_birth_date as client_birth_date,
        {{ coalesce_column(column='member_gender', alias='client_gender') }},
        {{ coalesce_column(column='member_address', alias='client_postal_address') }},
        {{ coalesce_column(column='member_phone_number', alias='client_phone_number') }},
        {{ coalesce_column(column='member_email', alias='client_email_address') }},
    
        {{ coalesce_column(column='provider_npi', replace_with=var("null_key_int")) }},
        {{ coalesce_column(column='provider_first_name') }},
        {{ coalesce_column(column='provider_last_name') }},

        {{ coalesce_column(column='service_cpt_code') }},
        {{ coalesce_column(column='service_name') }},
        
        -- dates
        service_start_date as service_started_at,
        service_end_date as service_ended_at,
        report_submission_date as report_submitted_at,
        created_at,
        updated_at,

        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    left join {{ ref('ref_payer_names') }} as payer 
        on rv.payer_id = payer.payer_id
    where true

),

{{ build_br_base_model(alias) }}

