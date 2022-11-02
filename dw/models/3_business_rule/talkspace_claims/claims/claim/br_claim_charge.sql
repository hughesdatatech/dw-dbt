{%- set jira_task_key = 'PLATFORM-1922' %}

with 

pp_charges as (

    select
        prepaid_charge_id
    from {{ ref('br_claims__claims_payments') }}
    where
        nvl(prepaid_charge_id, '') <> ''
    group by 1
),

final as (

    select
        tx.claim_id,
        tx.charge_id,
        case
            when pp.prepaid_charge_id is not null 
                then True
            else False
        end as is_prepaid_charge,
        sum(tx.transaction_amount) as charge_amount,
        case
            when count(1) > 1 
                then count(1)
            else 0
        end as duplicate_charge_count,
        sum(
            case
                when tx.transaction_amount <= 0.00
                    then 1
                else 0
            end
        ) as zero_negative_charge_count
    from {{ ref('br_claims__claims_transactions') }} as tx
    left join pp_charges as pp 
        on tx.charge_id = pp.prepaid_charge_id
    where
        tx.br_claims__claims_transactions_row_sqn_desc = 1
        and tx.charge_id is not null
        and tx.refund_id is null
    group by 1, 2, 3

)

select
    '{{ jira_task_key }}' as jira_task_key,
     {{ 
        build_hash_value(
            value=build_hash_diff(
                        cols=['claim_id', 'charge_id'],
                        boolean_cols=['is_prepaid_charge']
                    ),
            alias='br_claim_charge_hk'
        )
    }},
    *
from final
