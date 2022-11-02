{%- set jira_task_key = 'PLATFORM-1922' %}

with 

final as (

    select
        claim_id,
        refund_id,
        sum(transaction_amount) as refund_amount,
        sum(1) as duplicate_refund_count,
        sum(
            case
                when transaction_amount >= 0.00 
                    then 1
                else 0
            end
        ) as zero_positive_refund_count
    from {{ ref('br_claims__claims_transactions') }}
    where
        br_claims__claims_transactions_row_sqn_desc = 1
        and refund_id is not null
    group by 1, 2

)

select
    '{{ jira_task_key }}' as jira_task_key,
     {{ 
        build_hash_value(
            value=build_hash_diff(
                        cols=['claim_id', 'refund_id']
                    ),
            alias='br_claim_refund_hk'
        )
    }},
    *
from final
