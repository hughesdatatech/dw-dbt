
    
    

select
    member_hk as unique_field,
    count(*) as n_records

from dw_dev.dbt_steve.vim_member_all
where member_hk is not null
group by member_hk
having count(*) > 1


