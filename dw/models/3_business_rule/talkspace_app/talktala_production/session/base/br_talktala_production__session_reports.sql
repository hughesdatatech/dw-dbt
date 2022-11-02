{% set alias = this.name.replace('br_', '') %}

with 

rv_base as (

    select 
        -- business key
        id as session_report_id,
        
        -- fks
        {{ coalesce_column(column='case_id', replace_with=var("null_key_int")) }},
        {{ coalesce_column(column='room_id', replace_with=var("null_key_int")) }},
        {{ coalesce_column(column='session_modality_id', replace_with=var("null_key_int")) }},
        {{ coalesce_column(column='session_service_id', replace_with=var("null_key_int")) }},
        {{ coalesce_column(column='video_call_id', replace_with=var("null_key_int")) }},
        {{ coalesce_column(column='async_session_id', replace_with=var("null_key_int")) }},
        {{ coalesce_column(column='completed_by', replace_with=var("null_key_int"), alias='completed_by_provider_id') }},
        {{ coalesce_column(column='voucher_usage_id', replace_with=var("null_key_int")) }},
        {{ coalesce_column(column='gedi_payer_id', replace_with=var("null_key_char"), alias='payer_id') }},
        {{ coalesce_column(column='payment_transaction_id', replace_with=var("null_key_char")) }},
        {{ coalesce_column(column='progress_note_id', replace_with=var("null_key_int")) }},
        {{ coalesce_column(column='payment_type', replace_with=var("null_key_int")) }},

        -- misc
        {{ coalesce_column(column='position', replace_with=var("null_key_int"), alias='report_position') }},
        {{ coalesce_column(column='name', alias='report_name') }},
        {{ coalesce_column(column='notes', alias='report_notes') }},
        {{ coalesce_column(column='next_step') }},
        {{ coalesce_column(column='status', alias='report_status') }},
        {{ coalesce_column(column='therapist_timezone', alias='provider_timezone') }},
        {{ coalesce_column(column='is_automatic_submission', replace_with=0) }},

        -- dates
        completed_at as completed_at,
        start_date as started_at,
        end_date as ended_at,
        created_at,
        updated_at,
        reopened_at,
        locked_at,

        -- metrics
        therapy_duration_minutes as therapy_duration_minutes,
        maximum_cost_of_service as max_cost_of_service,
        {{ decimalize(column='copay_cents', alias='copay') }},
        word_count,

        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    where true

),

{{ build_br_base_model(alias) }}
