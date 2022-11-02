{{
    config(
        tags=['hold']
    )
}}

with final as
(
    select
        *
    from
        {{ ref('br_talktala_production_partner') }}
    where true
)
select
    *
from
    final
where true