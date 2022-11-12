{% set alias = this.name.replace('br_', '') %} -- Do not modify

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
        
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}  
    from {{ ref('rv_' + alias) }}
    where true

),

{{ build_br_base_model(alias) }}
