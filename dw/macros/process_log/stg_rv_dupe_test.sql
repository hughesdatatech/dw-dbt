{%
    macro stg_rv_dupe_test(
        tgt_table_name,
        stg_table_name,
        cols_for_uniqueness,
        test_number=0
    )
%}

with
SAT_KEY_DUPES_{{ stg_table_name }} as (
    select
        count(e) as key_dupes,
        '{{ cols_for_uniqueness|join(",") }}' as key_dupes_tgt_columns
    from (
        select
            count(*) as e
        from 
            {{ ref(tgt_table_name) }}
        group by
           {% for col in cols_for_uniqueness %}
                {{ col }}{% if not loop.last %},{% endif -%}
           {% endfor %}
        having
            count(*) > 1
    )
)
select
    '{{ invocation_id }}' as job_id,
    {{ remove_double_quotes(stg_table_name) }} as stg_table_name,
    {{ remove_double_quotes(tgt_table_name) }} as tgt_table_name,
    key_dupes,
    key_dupes_tgt_columns,
    {{ build_loaded_at(alias='null') }} as loaded_at,
    {{ build_inserted_at() }}
from 
    SAT_KEY_DUPES_{{ stg_table_name }}

{% endmacro %}
