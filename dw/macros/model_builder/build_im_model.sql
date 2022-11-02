{%- macro build_im_model(br_concept_model_name) -%}

{%- set concept_name = br_concept_model_name.replace('br_', '') %}

select
    {{
        build_dw_metadata_cols(
            unique_key=[br_concept_model_name + '_hk'],
            rec_source=ref(br_concept_model_name),
            alias='im_' + concept_name
        )  
    }},
    {{ build_dw_metadata_cols_concat('im_' + concept_name) }},
    *
from 
    {{ ref(br_concept_model_name) }}

{%- endmacro %}
