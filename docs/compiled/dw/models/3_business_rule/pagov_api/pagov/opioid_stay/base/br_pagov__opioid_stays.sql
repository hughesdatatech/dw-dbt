 -- Do not modify

with 

rv_base as (

    select

        -- pk
        county_name,
        time_period,

        -- fks

        -- misc
        fips_county_code,
        state_fips_code,
        county_fips_code,
        try_to_decimal(county_code_number, 14, 0) as county_code_number,
        case 
            when geocoded_column_type ilike 'point'
                then st_makepoint(
                        try_to_decimal(ltrim(split_part(geocoded_column_coordinates, ',', 1), '['), 13, 10), 
                        try_to_decimal(rtrim(split_part(geocoded_column_coordinates, ',', 2), ']'), 13, 10)
                    )
        end as geography_point,
        
        -- metrics
        try_to_decimal(count_of_maternal_stays_with, 14, 0) as maternal_stays_count,
        type_of_count as count_description,
        try_to_decimal(rate_of_maternal_stays_with, 14, 2) as rate_of_maternal_stays,
        type_of_rate as rate_description,

        -- dates
        try_to_date(time_period_date_start) as time_period_starting_at,
        try_to_date(time_period_date_end) as time_period_ending_at,

        ------------------------------------------
        -- Do not modify anything below this line.
        ------------------------------------------ 
        
        rv_pagov__opioid_stays_hk,
        rv_pagov__opioid_stays_loaded_at,
        dbt_scd_id as rv_pagov__opioid_stays_dbt_scd_id,
    dbt_updated_at as rv_pagov__opioid_stays_dbt_updated_at,
    dbt_valid_from as rv_pagov__opioid_stays_dbt_valid_from,
    dbt_valid_to as rv_pagov__opioid_stays_dbt_valid_to  
    from dw_dev.dbt_steve.rv_pagov__opioid_stays
    where true

),

rv_meta as (

    select
        rv.*,
        nvl(stat_track.rv_key_action, 'update') as rv_pagov__opioid_stays_key_action,
        nvl(stat_track.rv_key_status, 'active') as rv_pagov__opioid_stays_key_status,
        nvl(stat_track.rv_key_status_detected_at, rv.rv_pagov__opioid_stays_loaded_at) as rv_pagov__opioid_stays_key_status_detected_at
    from 
        rv_base as rv
    left join dw_dev.dbt_steve.process_log_key_status_tracking as stat_track
        on rv.rv_pagov__opioid_stays_hk = stat_track.rv___hk
        and rv.rv_pagov__opioid_stays_dbt_scd_id = stat_track.rv_dbt_scd_id
        and 'rv_' || 'pagov__opioid_stays' = stat_track.rec_source
        and nvl(stat_track.rv_key_action, '') <> 'delete'
    where true

    union all

    select
        rv.*,
        stat_track.rv_key_action,
        stat_track.rv_key_status,
        stat_track.rv_key_status_detected_at
    from 
        rv_base as rv
    inner join dw_dev.dbt_steve.process_log_key_status_tracking as stat_track
        on rv.rv_pagov__opioid_stays_hk = stat_track.rv___hk
        and rv.rv_pagov__opioid_stays_dbt_scd_id = stat_track.rv_dbt_scd_id
        and 'rv_' || 'pagov__opioid_stays' = stat_track.rec_source
        and stat_track.rv_key_action = 'delete'
    where true

),

final as (

    select
        cast(sha2(nvl(trim(cast(rv_pagov__opioid_stays_hk as varchar)), '') || '||' || nvl(trim(cast(rv_pagov__opioid_stays_dbt_scd_id as varchar)), '') || '||' || nvl(trim(cast(rv_pagov__opioid_stays_key_status as varchar)), '') || '||' || nvl(trim(cast(rv_pagov__opioid_stays_key_status_detected_at as varchar)), ''), 256) as varbinary(64)) br_pagov__opioid_stays_hk,
        *,
        row_number() over(partition by rv_pagov__opioid_stays_hk order by rv_pagov__opioid_stays_key_status_detected_at desc) br_pagov__opioid_stays_row_sqn_desc,
        'rv_pagov__opioid_stays_hk = ' || nvl(rv_pagov__opioid_stays_hk::varchar, '') || '; ' ||
    'rv_pagov__opioid_stays_loaded_at = ' || nvl(rv_pagov__opioid_stays_loaded_at::varchar, '') || '; ' ||
    'rv_pagov__opioid_stays_dbt_scd_id = ' || nvl(rv_pagov__opioid_stays_dbt_scd_id::varchar, '') || '; ' ||
    'rv_pagov__opioid_stays_dbt_updated_at = ' || nvl(rv_pagov__opioid_stays_dbt_updated_at::varchar, '') || '; ' ||
    'rv_pagov__opioid_stays_dbt_valid_from = ' || nvl(rv_pagov__opioid_stays_dbt_valid_from::varchar, '') || '; ' ||
    'rv_pagov__opioid_stays_dbt_valid_to = ' || nvl(rv_pagov__opioid_stays_dbt_valid_to::varchar, '') || '; ' ||
    'rv_pagov__opioid_stays_key_action = ' || nvl(rv_pagov__opioid_stays_key_action, '') || '; ' ||
    'rv_pagov__opioid_stays_key_status = ' || nvl(rv_pagov__opioid_stays_key_status, '') || '; ' ||
    'rv_pagov__opioid_stays_key_status_detected_at = ' || nvl(rv_pagov__opioid_stays_key_status_detected_at::varchar, '') || '; ' ||
    'br_pagov__opioid_stays_row_sqn_desc = ' || nvl(br_pagov__opioid_stays_row_sqn_desc::varchar, '') as rv_pagov__opioid_stays_metadata
    from 
        rv_meta
    where true

)

select *
from final
where true