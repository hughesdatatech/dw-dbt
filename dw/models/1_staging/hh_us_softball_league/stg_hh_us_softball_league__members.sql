with 

split_members as (

    select 
        split(tsv_value, '\t') as member_data
    from 
        {{ ref('ref_hh_us_softball_league__members') }}

),

all_members as (

    select
        {%- for column in var('us_softball_league_metadata') %}
            member_data[{{loop.index0}}]::{{ var('us_softball_league_metadata')[column][0] }} as {{ var('us_softball_league_metadata')[column][1] }}{% if not loop.last %},{% endif -%}
        {% endfor %}
    from 
        split_members

),

final as (

    select
        {#
            TO DO: leverage macros so that metadata and column names are more auto-generated.
        #}
        'default' as rv_hh_us_softball_league__members_tenant_key,
        'ref_hh_us_softball_league__members.tsv' as rv_hh_us_softball_league__members_rec_source,
        {{ build_job_id(invocation_id, 'rv_hh_us_softball_league__members') }},
        'circleci' as rv_hh_us_softball_league__members_job_user_id,
        'default' as rv_hh_us_softball_league__members_jira_task_key,
        {{ build_loaded_at(alias='null') }}  as rv_hh_us_softball_league__members_extracted_at,
        {{ build_loaded_at(alias='null') }} as rv_hh_us_softball_league__members_loaded_at,
        cast(sha2(nvl(trim(cast(id as varchar)), ''), 256) as varbinary(64)) as rv_hh_us_softball_league__members_hd,
        id,
        name,
        date_of_birth,
        company_id,
        last_active,
        score,
        date_from_parts(joined_league, 1, 1) as joined_league,
        us_state
    from 
        all_members

)

select
    cast(sha2(nvl(trim(cast(id as varchar)), '') || rv_hh_us_softball_league__members_rec_source, 256) as varbinary(64)) as rv_hh_us_softball_league__members_hk,
    *
from 
    final
