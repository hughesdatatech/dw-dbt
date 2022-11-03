{{
    config(
        materialized='incremental'
    )
}}

with 

all_keys_w_stat as (

{% if is_incremental() %}
      
    select
        *,
        row_number() over(partition by rv___hk, rec_source order by loaded_at desc) as row_sqn
    from 
        {{ this }}
    where true

{% else %}
      
    select
        null::varbinary(64) as rv___hk,
        null::varchar as rec_source,
        null::varchar as rv_key_status,
        null::integer as row_sqn

{% endif %}

),

latest_key_stat as (

    select *
    from 
        all_keys_w_stat
    where true
        and row_sqn = 1

),

{% for schema_key in var('stg_schema_config') %}

    {% for model_key in var('stg_' + schema_key + '__model_config') %}

    {{ model_key }}_rv as (

        select
            rv_{{ model_key }}_hk,
            dbt_scd_id,
            row_number() over(partition by rv_{{ model_key }}_hk order by dbt_updated_at desc) as row_sqn
        from 
            {{ ref('rv_' + model_key) }}

    ),

    {{ model_key }}_latest_rv as (

        select *
        from 
            {{ model_key }}_rv
        where true
            and row_sqn = 1

    ),

    {{ model_key }}_stat as (

        -- The purpose here is to flag and ultimately track first-time key insertions, deletions, and re-insertions to the raw vault (key = rv_[schema_name__table_name]_hk).
        -- If a key is not flagged it means the raw vault record was an update in which case we don't insert any record to the key status tracking table. 
        select 
            {{
                build_dw_metadata_cols(
                    rec_source='rv_' + model_key,
                    unique_key=['latest_rv.rv_' + model_key + '_hk'],
                    jira_task_key='null',
                    alias='rv_' + model_key
                )
            }},
            latest_rv.rv_{{ model_key }}_hk as rv___hk,
            latest_rv.dbt_scd_id as rv_dbt_scd_id,
            case
                when
                    ls.rv_key_status is null -- Raw vault record has never been tracked so it's a new insert.
                    then 'new_insert'
                when stg.rv_{{ model_key }}_hk is null and {{ get_model_config('stg_' + model_key, 'track_key_deletes') }}
                    and nvl(ls.rv_key_status, '') <> 'deleted'
                    then 'delete' -- Latest raw vault record IS NOT in staging and latest status is not deleted, so the action is a delete.
                when stg.rv_{{ model_key }}_hk is not null and {{ get_model_config('stg_' + model_key, 'track_key_deletes') }}
                    and nvl(ls.rv_key_status, '') = 'deleted'
                    then 're_insert' -- Latest raw vault records IS in staging and latest status is deleted, so the action is a re-insert.
                -- Otherwise it means the action is an update in which case we ignore.
            end as _rv_key_action,
            case
                when
                    _rv_key_action in ('new_insert', 're_insert')
                    then 'active'
                when
                    _rv_key_action = 'delete'
                    then 'deleted'
            end as _rv_key_status
        from 
            {{ model_key }}_latest_rv as latest_rv
            left join {{ ref('stg_' + model_key) }} stg
                on latest_rv.rv_{{ model_key }}_hk = stg.rv_{{ model_key }}_hk
            left join latest_key_stat ls on
                ls.rv___hk = latest_rv.rv_{{ model_key }}_hk
                and ls.rec_source = 'rv_' + '{{ model_key }}'

    )
    {% if not loop.last -%},{%- endif %}

    {% endfor %}

{% if not loop.last -%},{%- endif %}

{% endfor %}

{% for schema_key in var('stg_schema_config') %}

    {% for model_key in var('stg_' + schema_key + '__model_config') %}

        select 
            {{ 'rv_' + model_key + '_tenant_key' }} as tenant_key,
            {{ 'rv_' + model_key + '_hk' }} as hk,
            {{ 'rv_' + model_key + '_rec_source' }} as rec_source,
            {{ 'rv_' + model_key + '_job_id' }} as job_id,
            {{ 'rv_' + model_key + '_job_user_id' }} as job_user_id,
            {{ 'rv_' + model_key + '_jira_task_key' }} as jira_task_key,
            {{ 'rv_' + model_key + '_extracted_at' }} as extracted_at,
            {{ 'rv_' + model_key + '_loaded_at' }} as loaded_at,
            rv___hk,
            rv_dbt_scd_id,
            _rv_key_action as rv_key_action,
            _rv_key_status as rv_key_status,
            loaded_at as rv_key_status_detected_at
        from 
            {{ model_key }}_stat
        where true
            and _rv_key_action is not null

    {% if not loop.last -%}union all{%- endif %}

    {% endfor %}

{% if not loop.last -%}union all{%- endif %}

{% endfor %}
