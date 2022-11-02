{{
    config(
        tags=['hold']
    )
}}

with final as
(
	select 
		{{ ___TO_BE_DELETED() }},
		*,
		/* To handle potential duplicate records: Partition by primary key and order by dms cdc timestamp descending  */
		row_number()over(partition by plan_id order by _dms_cdc_ts desc, _dms_replication_ts desc) as stg_row_num
	from {{ source('talktala_production', 'plan_details') }}
)
select *
from final
where stg_row_num=1