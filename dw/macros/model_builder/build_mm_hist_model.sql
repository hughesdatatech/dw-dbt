{%- macro build_mm_hist_model(mm_model_name) -%}

{%- set concept_name = mm_model_name.replace('mm_', '') %}

select
    {{ concept_name }}.*
from 
    {{ ref('mm_' + concept_name) }} as {{ concept_name }}
{% if is_incremental() %}
    left join {{ this }} {{ concept_name }}_hist 
        on {{ concept_name }}.mm_{{ concept_name }}_hk = {{ concept_name }}_hist.mm_{{ concept_name }}_hk
where true
    and {{ concept_name }}_hist.mm_{{ concept_name }}_hk is null
{% endif %}

{%- endmacro %}
