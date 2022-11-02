{%- macro build_mm_pit_model(mm_model_name) -%}

select
    *,
    {{ mm_model_name }}_loaded_at as point_in_time_at
from 
    {{ ref(mm_model_name) }}

{%- endmacro %}
