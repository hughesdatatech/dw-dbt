
    with a as (
        
    select
        
        count(*) as expression
    from
        dw_dev.dbt_steve.vim_member_all
    where
        member_rec_source = 'ref_hh_us_softball_league__members.tsv'
    
    

    ),
    b as (
        
    select
        
        count(*) * 1 as expression
    from
        dw_dev.dbt_steve.ref_hh_us_softball_league__members
    where
        true
    
    

    ),
    final as (

        select
            
            a.expression,
            b.expression as compare_expression,
            abs(coalesce(a.expression, 0) - coalesce(b.expression, 0)) as expression_difference,
            abs(coalesce(a.expression, 0) - coalesce(b.expression, 0))/
                nullif(a.expression * 1.0, 0) as expression_difference_percent
        from
        
            a cross join b
        
    )
    -- DEBUG:
    -- select * from final
    select
        *
    from final
    where
        
        expression_difference > 0.0
        
