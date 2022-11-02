{%- macro build_im_hist_model(im_model_name) -%}

{%- set concept_name = im_model_name.replace('im_', '') %}

select
    {{ concept_name }}.*
from 
    {{ ref('im_' + concept_name) }} as {{ concept_name }}
{% if is_incremental() %}
    left join {{ this }} {{ concept_name }}_hist on 
        {{ concept_name }}.im_{{ concept_name }}_hk = {{ concept_name }}_hist.im_{{ concept_name }}_hk
where true
    and {{ concept_name }}_hist.im_{{ concept_name }}_hk is null
{% endif %}


{%- endmacro %}
