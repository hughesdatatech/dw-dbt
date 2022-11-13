{%- macro build_vim_hist_model(im_hist_model_name, unique_key=None) -%}

{%- set im_model_name = im_hist_model_name.rstrip('hist') -%}
{%- set hist_model_loaded_at = im_model_name + 'loaded_at' -%}
{%- set effective_end_date_col = im_model_name + 'effective_ending_at' -%}

{%- set concept_name = im_model_name.lstrip('im_') -%}
{%- set unique_key = im_model_name + 'hk'
        if unique_key is none
        else unique_key -%}

with

hist as (

    select
        *,
        {{ hist_model_loaded_at }} as {{ im_model_name + 'effective_starting_at' }},
        coalesce(
            lead({{ hist_model_loaded_at }}) over (
                partition by {{ unique_key }}
                order by {{ hist_model_loaded_at }}
            ),
            '{{ var("max_effective_date") }}'
        ) as {{ effective_end_date_col }}
    from {{ ref(im_hist_model_name) }}

)

select
    *,
    case
        when {{ effective_end_date_col }} = '{{ var("max_effective_date") }}' then true
        else false
    end as is_current_record
from hist

{% endmacro %}
