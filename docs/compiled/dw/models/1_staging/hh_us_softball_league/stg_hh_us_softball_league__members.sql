with 

split_members as (

    select 
        split(tsv_value, '\t') as member_data
    from 
        dw_dev.dbt_steve.ref_hh_us_softball_league__members

),

all_members as (

    select
            member_data[0]::int as id,
            member_data[1]::varchar(100) as name,
            member_data[2]::date as date_of_birth,
            member_data[3]::int as company_id,
            member_data[4]::date as last_active,
            member_data[5]::int as score,
            member_data[6]::int as joined_league,
            member_data[7]::varchar(50) as us_state
    from 
        split_members

),

final as (

    select
        
        'default' as rv_hh_us_softball_league__members_tenant_key,
        'ref_hh_us_softball_league__members.tsv' as rv_hh_us_softball_league__members_rec_source,
        '3749258b-3909-47c2-bc5b-edb0b79ebfc4' as rv_hh_us_softball_league__members_job_id,
        'circleci' as rv_hh_us_softball_league__members_job_user_id,
        'default' as rv_hh_us_softball_league__members_jira_task_key,
        to_char(timestamp '2023-05-07 19:40:26.907844+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp   as rv_hh_us_softball_league__members_extracted_at,
        to_char(timestamp '2023-05-07 19:40:26.907844+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp  as rv_hh_us_softball_league__members_loaded_at,
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