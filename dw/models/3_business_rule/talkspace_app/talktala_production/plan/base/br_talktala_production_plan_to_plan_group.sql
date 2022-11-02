{{
    config(
        tags=['hold']
    )
}}

with final as
(
    select
        id as plan_to_plan_group_id,
        plan_id,
        plan_group_id,
        /*{{ ___TO_BE_DELETED('plan_group') }},*/
        {{ build_dbt_metadata_cols('plan_group') }}
    from {{ ref('rv_talktala_production_plan_to_plan_group') }}
)
select *
from final