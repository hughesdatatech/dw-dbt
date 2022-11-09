{%- macro build_br_base_model(alias) -%}

rv_meta as (

    select
        rv.*,
        nvl(stat_track.rv_key_action, 'update') as rv_{{ alias }}_key_action,
        nvl(stat_track.rv_key_status, 'active') as rv_{{ alias }}_key_status,
        nvl(stat_track.rv_key_status_detected_at, rv.rv_{{ alias }}_loaded_at) as rv_{{ alias }}_key_status_detected_at
    from 
        rv_base as rv
    left join {{ ref('process_log_key_status_tracking') }} as stat_track
        on rv.rv_{{ alias }}_hk = stat_track.rv___hk
        and rv.rv_{{ alias }}_dbt_scd_id = stat_track.rv_dbt_scd_id
        and 'rv_' || '{{ alias }}' = stat_track.rec_source
        and nvl(stat_track.rv_key_action, '') <> 'delete'
    where true

    union all

    select
        rv.*,
        stat_track.rv_key_action,
        stat_track.rv_key_status,
        stat_track.rv_key_status_detected_at
    from 
        rv_base as rv
    inner join {{ ref('process_log_key_status_tracking') }} as stat_track
        on rv.rv_{{ alias }}_hk = stat_track.rv___hk
        and rv.rv_{{ alias }}_dbt_scd_id = stat_track.rv_dbt_scd_id
        and 'rv_' || '{{ alias }}' = stat_track.rec_source
        and stat_track.rv_key_action = 'delete'
    where true

),

final as (

    select
        {{ 
            build_hash_value(
                value=build_hash_diff(
                            cols=['rv_' + alias + '_hk', 'rv_' + alias + '_dbt_scd_id', 'rv_' + alias + '_key_status', 'rv_' + alias + '_key_status_detected_at']
                        ),
                alias='br_' + alias + '_hk'
            )
        }},
        *,
        {{ build_row_sqn(alias_prefix='br_' + alias + '_row_sqn_desc', partition_list='rv_' + alias + '_hk', order_by='rv_' + alias + '_key_status_detected_at desc') }},
        {{ build_rv_metadata_cols_concat(alias) }}
    from 
        rv_meta
    where true

)

select *
from final
where true

{%- endmacro %}
