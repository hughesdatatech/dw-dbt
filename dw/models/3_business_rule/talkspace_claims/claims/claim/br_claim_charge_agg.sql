select
    claim_id,
    is_prepaid_charge,
    sum(charge_amount) as charge_amount,
    sum(duplicate_charge_count) as duplicate_charge_count,
    sum(zero_negative_charge_count) as zero_negative_charge_count
from {{ ref('br_claim_charge') }}
group by 1, 2
