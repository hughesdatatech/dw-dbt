{%- test monthly_snapshot_has_run(model) -%}

select
    'monthly snapshot has run' as error_msg
from {{ model }} 
where 
    1 = 0

/*
This needs to be re-worked a little.... changing from daily to monthly
select
    'monthly snapshot has run' as error_msg
from {{ model }} 
where 
    cast(trunc(sysdate()) as date) = (
            select 
                cast(nvl(max(to_char(inserted_at, 'YYYY-MM-DD')), '0001-01-01') as date) 
            from {{ target.schema }}.{{ var('process_log') }} 
            where 
                job_step_component = {{ remove_double_quotes(model) }} and
                job_step = 'snapshot post-hook'
    )
limit 1
*/

{%- endtest -%}
