{%- test process_log_load_succeeded(model, column_name) -%}

select
    *
from 
    {{ model }}
where true
    and {{ column_name }} != 0
    and inserted_at = (
        select max(inserted_at) from {{ model }}
    )

{%- endtest -%}
