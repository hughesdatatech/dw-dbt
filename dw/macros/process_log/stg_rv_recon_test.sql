{%
    macro stg_rv_recon_test(
        tgt_table_name,
        stg_table_name,
        cols_for_uniqueness,
        strategy='timestamp',
        test_number=0
    )
%}

with 
SAT_UKEY_SGTG_{{ stg_table_name }}
as (
  select 
    count(1) as unique_key_tgt_not_loaded,
    '{{ ','.join(cols_for_uniqueness) }}' as unique_key_stg_columns,
    '{{ ','.join(cols_for_uniqueness) }}' as unique_key_tgt_columns
  from 
    {{ ref(stg_table_name) }} as sg 
  where not exists
  (
    select 
      1
    from 
      {{ ref(tgt_table_name) }} as tg
    where 
      {% for column_name in cols_for_uniqueness -%}
        nvl(sg.{{ column_name }}::varchar, '') = nvl(tg.{{ column_name }}::varchar, '') {% if not loop.last -%} and {% endif %}
      {%- endfor %}
      {% if strategy == 'check' -%} and dbt_valid_to is null{% endif %}
      /*
        For the check strategy, hard-deletes can't be invalidated. You want to compare the staged data against the latest target (raw vault) records.
        This will handle a situation (unlikely?) where data changed but then changed back to a prior value, e.g. a to b back to a.
        The latest records when using the check strategy are identifiable where dbt_valid_to is null.
        For the timestamp strategy

      */ 
  )
), 
FETCH_SAT_STATS1_{{ stg_table_name }}
as (
  select
    nvl(sum(cnt), 0) as unique_key_stg_gross,
    count(1) as unique_key_stg_unique
  from (
    select
      count(*) as cnt
    from 
      {{ ref(stg_table_name) }}
    group by
      {% for col in cols_for_uniqueness -%}
          {{ col }}{% if not loop.last %},{% endif -%}
      {%- endfor %}
  )
), 
FETCH_SAT_STATS2_{{ stg_table_name }}
as (
    select 
      count(1) as unique_key_tgt_loaded
    from (
      select 
        {% for column_name in cols_for_uniqueness -%}
            sg.{{ column_name }} {% if not loop.last -%}, {% endif %}
        {%- endfor %}
      from 
        {{ ref(stg_table_name) }} as sg
      where exists 
      (
        select 
          1
        from {{ ref(tgt_table_name) }} as tg
        where 
          sg.{{ tgt_table_name }}_job_id = tg.{{ tgt_table_name }}_job_id and
          {% for column_name in cols_for_uniqueness -%}
            nvl(sg.{{ column_name }}::varchar, '') = nvl(tg.{{ column_name }}::varchar, '') {% if not loop.last -%} and {% endif %}
          {%- endfor %} 
      )
      group by
        {% for column_name in cols_for_uniqueness -%}
            sg.{{ column_name }} {% if not loop.last -%}, {% endif %}
        {%- endfor %}
    )
),
FETCH_SAT_STATS3_{{ stg_table_name }}
as (
  select 
    count(1) unique_key_tgt_total
  from {{ ref(tgt_table_name) }}
 )
 select 
  '{{ invocation_id }}' as job_id,
  {{ remove_double_quotes(stg_table_name) }} as stg_table_name,
  {{ remove_double_quotes(tgt_table_name) }} as tgt_table_name,
  unique_key_stg_columns,
  unique_key_tgt_columns,
  unique_key_stg_gross,
  unique_key_stg_unique,
  unique_key_tgt_not_loaded,
  unique_key_tgt_loaded,
  unique_key_tgt_total,
  {{ build_loaded_at(alias='null') }} as loaded_at,
  {{ build_inserted_at() }}
from 
  SAT_UKEY_SGTG_{{ stg_table_name }}, 
  FETCH_SAT_STATS1_{{ stg_table_name }}, 
  FETCH_SAT_STATS2_{{ stg_table_name }}, 
  FETCH_SAT_STATS3_{{ stg_table_name }}

{% endmacro %}