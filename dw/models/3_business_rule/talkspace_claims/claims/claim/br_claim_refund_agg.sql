select
    claim_id,
    sum(refund_amount) as refund_amount,
    sum(duplicate_refund_count) as duplicate_refund_count,
    sum(zero_positive_refund_count) as  zero_positive_refund_count
from {{ ref('br_claim_refund') }}
group by 1
