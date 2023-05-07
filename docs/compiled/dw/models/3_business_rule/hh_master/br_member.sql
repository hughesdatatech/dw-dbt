with

us_softball as (

    select
        memb.rv_hh_us_softball_league__members_tenant_key as member_tenant_key,
        memb.rv_hh_us_softball_league__members_hk as member_hk,
        memb.rv_hh_us_softball_league__members_rec_source as member_rec_source,
        memb.rv_hh_us_softball_league__members_job_id as member_job_id,
        memb.rv_hh_us_softball_league__members_job_user_id as member_job_user_id,
        memb.rv_hh_us_softball_league__members_jira_task_key as member_jira_task_key,
        memb.rv_hh_us_softball_league__members_extracted_at as member_extracted_at,
        memb.rv_hh_us_softball_league__members_loaded_at as member_loaded_at,
        memb.rv_hh_us_softball_league__members_hd as member_hd,
        memb.id as member_id,
        
        split(memb.name, ' ')[0]::varchar(50) as first_name,
        split(memb.name, ' ')[1]::varchar(50) as last_name,
        memb.date_of_birth as birth_date,
        memb.company_id,
        memb.last_active as last_active_at,
        memb.score,
        memb.joined_league as joined_at,
        
        upper(
            case
                when split(memb.us_state, ' ')[1] is not null
                    then left(split(memb.us_state, ' ')[0], 1) || left(split(memb.us_state, ' ')[1], 1)
                else left(memb.us_state, 2)
            end
         ) as state_code
    from 
        dw_dev.dbt_steve.stg_hh_us_softball_league__members as memb
    where true
    
),

unity_golf as (

    select
        memb.rv_hh_unity_golf_club__members_tenant_key as member_tenant_key,
        memb.rv_hh_unity_golf_club__members_hk as member_hk,
        memb.rv_hh_unity_golf_club__members_rec_source as member_rec_source,
        memb.rv_hh_unity_golf_club__members_job_id as member_job_id,
        memb.rv_hh_unity_golf_club__members_job_user_id as member_job_user_id,
        memb.rv_hh_unity_golf_club__members_jira_task_key as member_jira_task_key,
        memb.rv_hh_unity_golf_club__members_extracted_at as member_extracted_at,
        memb.rv_hh_unity_golf_club__members_loaded_at as member_loaded_at,
        memb.rv_hh_unity_golf_club__members_hd as member_hd,
        memb.id as member_id,
        memb.first_name,
        memb.last_name,
        memb.dob as birth_date,
        memb.company_id,
        memb.last_active as last_active_at,
        memb.score,
        memb.member_since as joined_at,
        state as state_code
    from 
        dw_dev.dbt_steve.stg_hh_unity_golf_club__members as memb
    where true


),

combined as (

    select 
        * 
    from us_softball
    where true

    union all

    select
        *
    from unity_golf
    where true

),

final as (

    select
        combined.member_tenant_key,
        combined.member_hk,
        combined.member_rec_source,
        combined.member_job_id,
        combined.member_job_user_id,
        combined.member_jira_task_key,
        combined.member_extracted_at,
        combined.member_loaded_at,
        combined.member_hd,
        combined.member_id,
        combined.first_name,
        combined.last_name,
        combined.birth_date,
        
        nvl(comp.name, 'Unknown') as company_name,
        combined.last_active_at,
        combined.score,
        combined.joined_at,
        combined.state_code
    from
        combined 
        
        left join dw_dev.dbt_steve.stg_hh_master__companies as comp 
            on combined.company_id = comp.id
    where true

)

select *
from final
where true