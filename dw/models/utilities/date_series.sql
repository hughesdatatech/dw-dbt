
{%- set start_date = "cast('" ~ var('dim_date_valid_range')[1] ~ "' as date)" -%}
{%- set end_date = "cast('" ~ var('dim_date_valid_range')[2] ~ "' as date)" -%} 

with final as
(
	select 
		date_full,
		/* year fields */
		extract(year from date_full)::int as year_number,
		
		/* quarter fields */
		extract(quarter from date_full)::int as quarter_number,
		'Q' + extract(quarter from date_full)::varchar as quarter_name,
		
		/* year month */
		extract(month from date_full)::int as month_number,
		trim(to_char(date_full , 'Month')) as month_name,
		trim(to_char(date_full , 'Mon')) as month_name_abbr,
		
		/* week fields */
		trim(to_char(date_full , 'W'))::int as week_of_month_number,
		trim(to_char(date_full , 'WW'))::int as week_of_year_number,
		
		/* day fields */
		extract(day from date_full)::int as day_of_month_number,
		trim(to_char(date_full , 'D'))::int as day_of_week_number,
		trim(to_char(date_full , 'DDD'))::int as day_of_year_number,
		trim(to_char(date_full , 'Day')) as day_name,
		trim(to_char(date_full , 'Dy')) as day_name_abbr
	from
	(
		/* use redshift generate_series function to calculate dates */
		select dateadd(day , gs , {{ end_date }} )::date as date_full	
		from generate_series(0,datediff(day , {{ end_date }} , {{ start_date }}),-1) as gs

	) as calendar
)
select *
from final
order by 1 desc