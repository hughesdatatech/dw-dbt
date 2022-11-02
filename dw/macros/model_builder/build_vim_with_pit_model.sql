{%- macro build_vim_with_pit_model(im_model_name) -%}

{% set relation_exists = (load_relation(ref(im_model_name + '_pit'))) is not none %}

{% if relation_exists %}
select
    *,
    {{ dbt_date.convert_timezone("point_in_time_at", "America/New_York") }} as point_in_time_at_et,
    True as is_point_in_time
from 
    {{ ref(im_model_name + '_pit') }}

union all
{% endif %}

select
    *,
    {{ im_model_name }}_loaded_at as point_in_time_at,
    {{ dbt_date.convert_timezone(im_model_name + "_loaded_at", "America/New_York") }} as point_in_time_at_et,
    False as is_point_in_time
from 
    {{ ref(im_model_name) }}

{%- endmacro %}
