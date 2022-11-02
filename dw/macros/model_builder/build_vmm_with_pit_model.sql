{%- macro build_vmm_with_pit_model(mm_model_name) -%}

{% set relation_exists = (load_relation(ref(mm_model_name + '_pit'))) is not none %}

{% if relation_exists %}
select
    *,
    {{ dbt_date.convert_timezone("point_in_time_at", "America/New_York") }} as point_in_time_at_et,
    True as is_point_in_time
from 
    {{ ref(mm_model_name + '_pit') }}

union all
{% endif %}

select
    *,
    {{ mm_model_name }}_loaded_at as point_in_time_at,
    {{ dbt_date.convert_timezone(mm_model_name + "_loaded_at", "America/New_York") }} as point_in_time_at_et,
    False as is_point_in_time
from 
    {{ ref(mm_model_name) }}

{%- endmacro %}
