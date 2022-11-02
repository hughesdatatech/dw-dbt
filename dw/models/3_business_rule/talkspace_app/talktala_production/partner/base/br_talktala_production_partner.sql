{{
    config(
        tags=['hold']
    )
}}

with final as
(
    select
        id as partner_id,
        name as partner_name,
        is_eligibility_based_on_file::boolean,
        default_plan_id,
        /*{{ ___TO_BE_DELETED('partner') }},*/
        {{ build_dbt_metadata_cols('partner') }}
    from
        {{ ref('rv_talktala_production_partner') }}
    where true
)
select
    *
from
    final
where true
