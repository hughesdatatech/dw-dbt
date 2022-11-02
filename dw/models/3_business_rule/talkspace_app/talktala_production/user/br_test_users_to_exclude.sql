{{
    config(
        tags=['hold']
    )
}}

with final as
(
	select user_id
	from {{ ref('br_talktala_production_users') }}
    /*where dbt_valid_to is null
    and is_test_user is true*/
)
select *
from final