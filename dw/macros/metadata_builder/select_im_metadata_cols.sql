{% macro select_im_metadata_cols(im_name='', rv_name='', use_null_value=False) -%}
    {% if use_null_value==True %}
        null::varbinary(64) as {{ im_name ~ '_hk' }},
        null::varchar as {{ im_name ~ '_metadata' }},
        null::varchar as {{ rv_name ~ '_metadata' }}
    {% else %}
        {{ im_name ~ '_hk' }},
        {{ im_name ~ '_metadata' }},
        {{ rv_name ~ '_metadata' }}
    {% endif%}
{%- endmacro %}