{%- set jira_task_key = 'PLATFORM-1891' %}

{% set relation_exists = (load_relation(ref('rv_stripe_mobile__invoices_metadata'))) is not none %}
{% if relation_exists %}
      {% set pivot_values = dbt_utils.get_column_values(ref('br_stripe_mobile__invoices_metadata'), 'metadata_key') %}
{% else %}
      {% set pivot_values = [] %}
{% endif %}

with

listagg_len_check as ( -- Used to ensure listagg won't fail if combined metadata columns length exceeds limit.

    select
        invoice_id,
        sum(len(rv_stripe_mobile__invoices_metadata_metadata)) as rv_stripe_mobile__invoices_metadata_metadata_len
    from 
        {{ ref('br_stripe_mobile__invoices_metadata') }}
    where
        br_stripe_mobile__invoices_metadata_row_sqn_desc = 1
    group by 1

),

meta as (

    select
        br_stripe_mobile__invoices_metadata_hk,
        invoice_id,
        metadata_key,
        case
            when rv_stripe_mobile__invoices_metadata_key_status = 'deleted'
                then ''
            else metadata_value
        end as metadata_value,
        rv_stripe_mobile__invoices_metadata_metadata
    from 
        {{ ref('br_stripe_mobile__invoices_metadata') }}
    where
        br_stripe_mobile__invoices_metadata_row_sqn_desc = 1

),

final as (

    select
        meta.invoice_id,
        listagg(br_stripe_mobile__invoices_metadata_hk::varchar, '||') as br_invoice_metadata_pivot_hk_list,
        listagg(
            case
                when rv_stripe_mobile__invoices_metadata_metadata_len > 65535
                    then ''
                else
                    rv_stripe_mobile__invoices_metadata_metadata
            end, '; '
         ) as rv_stripe_mobile__invoices_metadata_metadata,
        {{ dbt_utils.pivot(
            column='metadata_key',
            values=pivot_values,
            agg='max',
            then_value='metadata_value',
            else_value="''"
        ) }}
    from 
        meta
    inner join listagg_len_check lc 
        on nvl(meta.invoice_id, '') = nvl(lc.invoice_id, '')
    group by 1

)

select
    '{{ jira_task_key }}' as jira_task_key,
     {{ 
        build_hash_value(
            value=build_hash_diff(
                        cols=['br_invoice_metadata_pivot_hk_list']
                    ),
            alias='br_invoice_metadata_pivot_hk'
        )
    }},
    *
from 
    final
