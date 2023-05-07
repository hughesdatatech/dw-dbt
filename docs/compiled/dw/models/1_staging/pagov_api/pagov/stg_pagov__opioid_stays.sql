

with

all_source_rows as (

    select
        'default' as rv_pagov__opioid_stays_tenant_key,
    cast(sha2(nvl(trim(cast(county_name as varchar)), '') || '||' || nvl(trim(cast(time_period_date_end as varchar)), ''), 256) as varbinary(64)) rv_pagov__opioid_stays_hk,
    
        'dw_dev.pagov.opioid_stays' as rv_pagov__opioid_stays_rec_source,
    
    'dcb601de-b39f-4733-a1b9-29ff8c82de68' as rv_pagov__opioid_stays_job_id,
    'circleci' as rv_pagov__opioid_stays_job_user_id,
    'default' as rv_pagov__opioid_stays_jira_task_key,
    to_char(timestamp '2023-05-07 20:19:17.787862+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp  as rv_pagov__opioid_stays_extracted_at,
    to_char(timestamp '2023-05-07 20:19:17.787862+00:00', 'YYYY-MM-DD HH24:MI:SS')::timestamp rv_pagov__opioid_stays_loaded_at,
    cast(sha2(nvl(trim(cast(COUNT_OF_MATERNAL_STAYS_WITH as varchar)), '') || '||' || nvl(trim(cast(COUNTY_CODE_NUMBER as varchar)), '') || '||' || nvl(trim(cast(COUNTY_FIPS_CODE as varchar)), '') || '||' || nvl(trim(cast(FIPS_COUNTY_CODE as varchar)), '') || '||' || nvl(trim(cast(GEOCODED_COLUMN_COORDINATES as varchar)), '') || '||' || nvl(trim(cast(GEOCODED_COLUMN_TYPE as varchar)), '') || '||' || nvl(trim(cast(LATITUDE_LONGITUDE as varchar)), '') || '||' || nvl(trim(cast(RATE_OF_MATERNAL_STAYS_WITH as varchar)), '') || '||' || nvl(trim(cast(STATE_FIPS_CODE as varchar)), '') || '||' || nvl(trim(cast(TIME_PERIOD as varchar)), '') || '||' || nvl(trim(cast(TIME_PERIOD_DATE_START as varchar)), '') || '||' || nvl(trim(cast(TYPE_OF_COUNT as varchar)), '') || '||' || nvl(trim(cast(TYPE_OF_RATE as varchar)), ''), 256) as varbinary(64)) rv_pagov__opioid_stays_hd
    ,
        COUNT_OF_MATERNAL_STAYS_WITH, 
		COUNTY_CODE_NUMBER, 
		COUNTY_FIPS_CODE, 
		COUNTY_NAME, 
		FIPS_COUNTY_CODE, 
		GEOCODED_COLUMN_COORDINATES, 
		GEOCODED_COLUMN_TYPE, 
		LATITUDE_LONGITUDE, 
		RATE_OF_MATERNAL_STAYS_WITH, 
		STATE_FIPS_CODE, 
		TIME_PERIOD, 
		TIME_PERIOD_DATE_END, 
		TIME_PERIOD_DATE_START, 
		TYPE_OF_COUNT, 
		TYPE_OF_RATE,
        
        
        row_number() over (
            partition by county_name,time_period_date_end
            order by 1 desc
        ) as row_num
    from 
        dw_dev.pagov.opioid_stays
        
)

select
    rv_pagov__opioid_stays_tenant_key,
    rv_pagov__opioid_stays_hk,
    rv_pagov__opioid_stays_rec_source,
    rv_pagov__opioid_stays_job_id,
    rv_pagov__opioid_stays_job_user_id,
    rv_pagov__opioid_stays_jira_task_key,
    rv_pagov__opioid_stays_extracted_at,
    rv_pagov__opioid_stays_loaded_at
    , rv_pagov__opioid_stays_hd,
    COUNT_OF_MATERNAL_STAYS_WITH, 
	COUNTY_CODE_NUMBER, 
	COUNTY_FIPS_CODE, 
	COUNTY_NAME, 
	FIPS_COUNTY_CODE, 
	GEOCODED_COLUMN_COORDINATES, 
	GEOCODED_COLUMN_TYPE, 
	LATITUDE_LONGITUDE, 
	RATE_OF_MATERNAL_STAYS_WITH, 
	STATE_FIPS_CODE, 
	TIME_PERIOD, 
	TIME_PERIOD_DATE_END, 
	TIME_PERIOD_DATE_START, 
	TYPE_OF_COUNT, 
	TYPE_OF_RATE
    
    
from 
    all_source_rows
where true
    and row_num = 1 
