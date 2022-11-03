{%- set jira_task_key = 'PLATFORM-1891' %}

with

final as (
        12345 as val,
        -- metadata
        {{ select_im_metadata_cols(im_name='im_demo_concept', rv_name='rv_demo_schema__demo_table', use_null_value=False) }}

)

select 
    '{{ jira_task_key }}' as jira_task_key,
    {{ 
        build_hash_value(
            value=build_hash_diff(
                        cols=[
                                'im_demo_concept_hk'
                            ]
                    ),
            alias='mr_demo_concept_hk'
        )
    }},
    *
from final
