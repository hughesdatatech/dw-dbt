select *
from {{ ref('br_member_exception') }}
where true
