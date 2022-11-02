{%- macro build_im_pit_model(im_model_name) -%}

select
    *,
    {{ im_model_name }}_loaded_at as point_in_time_at
from 
    {{ ref(im_model_name) }}

{%- endmacro %}
