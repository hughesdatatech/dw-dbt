{%- set jira_task_key = 'PLATFORM-1891' %}

with 

listagg_len_check as ( -- Used to ensure listagg won't fail if combined metadata columns length exceeds limit.

    select
        invoice_id,
        sum(len(rv_stripe_mobile__invoice_items_metadata)) as rv_stripe_mobile__invoice_items_metadata_len
    from {{ ref('br_invoice_item') }}
    group by 1

),

final as (

    select
        ii.invoice_id,
        listagg(
            case 
                when rv_stripe_mobile__invoice_items_key_status = 'deleted'
                    then ''
                else invoice_item_description
            end, '; '
        ) as invoice_item_list,
        listagg(br_invoice_item_hk::varchar, '||') as br_invoice_item_hk_list,
        listagg(
            case
                when rv_stripe_mobile__invoice_items_metadata_len > 65535
                    then ''
                else
                    rv_stripe_mobile__invoice_items_metadata
            end, '; '
         ) as rv_stripe_mobile__invoice_items_metadata
    from {{ ref('br_invoice_item') }} ii
    inner join listagg_len_check lc 
        on nvl(ii.invoice_id, '') = nvl(lc.invoice_id, '')
    group by 1

)

select
    '{{ jira_task_key }}' as jira_task_key,
    {{ 
        build_hash_value(
            value=build_hash_diff(
                        cols=['br_invoice_item_hk_list']
                    ),
            alias='br_invoice_item_pivot_hk'
        )
    }},
    *
from final
