{%- macro build_mm_model(mr_model_name) -%}

{%- set metrics_name = mr_model_name.replace('mr_', '') %}

select
    {{
        build_dw_metadata_cols(
            unique_key=[mr_model_name + '_hk'],
            rec_source=ref(mr_model_name),
            alias='mm_' + metrics_name
        )  
    }},
    {{ build_dw_metadata_cols_concat('mm_' + metrics_name) }},
    *
from 
    {{ ref(mr_model_name) }}

{%- endmacro %}
