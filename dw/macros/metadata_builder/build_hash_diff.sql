{%- macro build_hash_diff(cols, boolean_cols=[], namesafe_cols=[]) -%}
    {%- for col in cols -%}
        nvl(trim(cast({{ col }} as varchar)), '') {%- if not loop.last %} || '{{ var("sanding_value") }}' || {% endif %}
    {%- endfor -%}
    {%- if boolean_cols|length > 0 %}
        || '{{ var("sanding_value") }}' || 
        {%- for col in boolean_cols -%}
            nvl(trim(cast({{ col }}::smallint as varchar)), '') {%- if not loop.last %} || '{{ var("sanding_value") }}' || {% endif %}
        {%- endfor -%}
    {% endif %}
    {%- if namesafe_cols|length > 0 %}
        || '{{ var("sanding_value") }}' || 
        {%- for col in namesafe_cols -%}
            nvl(trim(cast({{ col }} as varchar)), '') {%- if not loop.last %} || '{{ var("sanding_value") }}' || {% endif %}
        {%- endfor -%}
    {% endif %}
{%- endmacro -%}
