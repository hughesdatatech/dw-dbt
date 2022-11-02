{% set alias = this.name.replace('br_', '') %}

with 

rv_base as (

    select 
        -- business key
        id as claim_id,

        -- fks
        {{ coalesce_column(column='session_report_id', replace_with=var("null_key_int")) }},
        {{ coalesce_column(column='member_user_id', replace_with=var("null_key_int"), alias='client_id') }},
        {{ coalesce_column(column='member_room_id', replace_with=var("null_key_int"), alias='room_id') }},
        {{ coalesce_column(column='provider_user_id', replace_with=var("null_key_int"), alias='provider_id') }},
        {{ coalesce_column(column='claim_data_id', replace_with=var("null_key_int"), alias='claim_data_id') }},
        {{ coalesce_column(column='claim_submitted_id', replace_with=var("null_key_int"), alias='claim_submission_id') }},
        {{ coalesce_column(column='claim_payment_id', replace_with=var("null_key_int"), alias='claim_payment_id') }},
        {{ coalesce_column(column='assignee_user_id', replace_with=var("null_key_int"), alias='admin_id') }},

        -- misc
        {{ coalesce_column(column='status', alias='claim_status') }},
        {{ coalesce_column(column='status_reason', alias='claim_status_reason') }},
        {{ coalesce_column(column='notes') }},
        {{ coalesce_column(column='is_test', replace_with=0, alias='is_test_claim') }},
        {{ coalesce_column(column='is_escalated', replace_with=0, alias='is_escalated_claim') }},

        -- dates
        created_at,
        
        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    where true

),

{{ build_br_base_model(alias) }}
