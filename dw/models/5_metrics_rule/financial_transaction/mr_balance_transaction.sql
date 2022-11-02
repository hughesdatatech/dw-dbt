{%- set jira_task_key = 'PLATFORM-1891' %}

with

transactions as (

    select
        balance_transaction_id,
        source_id,
        created_at,
        date_trunc('month', created_at) as month_created_at,
        balance_transaction_description,
        reporting_category,
        gross_amount,
        fee_amount,
        net_amount,
        im_balance_transaction_hk,
        im_balance_transaction_metadata,
        rv_stripe_mobile__balance_transactions_metadata
    from
        {{ ref('im_balance_transaction') }}
    where true
        and reporting_category not in ('payout')
        and balance_transaction_type not like '%transfer%'
    
),

charges as (
        
    select
        trx.balance_transaction_id,
        trx.source_id as charge_id,
        null as refund_id,
        null as dispute_id,
        trx.created_at,
        trx.month_created_at,
        trx.balance_transaction_description,
        trx.reporting_category,
        trx.gross_amount,
        trx.fee_amount,
        trx.net_amount,
        inv.invoice_id,
        met.claimID as claim_id,
        sub.subscription_id,
        sub.plan_id,
        chg.customer_id,
        chg.charge_description,
        chg.created_at as original_charge_at,
        case
            when met.chargeType is not null 
                then met.chargeType
            when sub.subscription_id is not null 
                or chg.charge_description ilike '%unlimited messaging therapy 1 month%'
                then 'subscription'
            when chg.charge_description ilike '%one time charge%'
                or chg.charge_description ilike '%lvs credit%'
                or chg.charge_description ilike '%live video session%'
                or chg.charge_description ilike '%live video credit%'
                or chg.charge_description ilike '%psychiatry - live video credit%'
                or chg.charge_description ilike '%therapy_live_video_credit%'
                or chg.charge_description ilike '%video_only_therapy%'
                then 'onetime'
            when chg.charge_description ilike '%copay%'
                then 'copay'
            when chg.charge_description ilike '%service%'
                or chg.charge_description ilike '%dos %'
                or iit.invoice_item_list ilike '%service%'
                or iit.invoice_item_list ilike '%dos %'
                or iit.invoice_item_list ilike '%date of %'
                or iit.invoice_item_list ilike '%payment plan for 2020 sessions%'
                or iit.invoice_item_list ilike '%follow-up appointment%'
                or iit.invoice_item_list ilike '%initial evaluation appointment%'
                then 'post-session'
            else 'unknown'
        end as charge_type,
        lower(charge_type) as lc_charge_type,
        case
            when lc_charge_type in (
                'copay', 'post-session',
                'noshow', 'latecancellation'
            ) then 'B2B'
            when lc_charge_type in (
                'subscription', 'onetime'
            ) then 'B2C'
            else 'unknown'
        end as business_line,
        iit.invoice_item_list,
        case 
            when charge_type = 'post-session'
                and claim_id is null
                then True
            else False
        end as is_psp_no_claim,
        case
            when is_psp_no_claim
                then 
                coalesce(
                    chg.charge_description, 
                    ''
                ) + ' ' +
                coalesce(
                    iit.invoice_item_list,
                    ''
                ) 
        end as psp_combined_description,
        case
            when is_psp_no_claim
                then trim(regexp_replace(psp_combined_description, '[^/\\d]', ' '))
            else null 
        end as psp_clean_description,
        regexp_substr(
            psp_clean_description, 
            '\\d\\d?(/)\\d\\d?(/)(\\d\\d)+', 
            1, 
            regexp_count(psp_clean_description, '\\d\\d?(/)\\d\\d?(/)(\\d\\d)+')
        ) as psp_date_candidate_str,
        case
            when len(split_part(psp_date_candidate_str, '/', 3)) = 2 
                then to_date(psp_date_candidate_str, 'MM-DD-YY')
            when len(split_part(psp_date_candidate_str, '/', 3)) = 4 
                then to_date(psp_date_candidate_str, 'MM-DD-YYYY')
            else null
        end as psp_date_candidate,
        case
            when is_psp_no_claim
                then greatest(psp_date_candidate, to_date('2000-01-01', 'YYYY-MM-DD'))
            else null
        end as psp_parsed_dos_at,
        -- metadata
        {{ select_im_metadata_cols(im_name='im_balance_transaction', rv_name='rv_stripe_mobile__balance_transactions') }},
        {{ select_im_metadata_cols(im_name='im_charge', rv_name='rv_stripe_mobile__charges') }},
        {{ select_im_metadata_cols(im_name='im_refund', rv_name='rv_stripe_mobile__refunds', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_invoice', rv_name='rv_stripe_mobile__invoices') }},
        {{ select_im_metadata_cols(im_name='im_subscription', rv_name='rv_stripe_mobile__subscriptions') }},
        {{ select_im_metadata_cols(im_name='im_dispute', rv_name='rv_stripe_mobile__disputes', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_invoice_item_pivot', rv_name='rv_stripe_mobile__invoice_items') }},
        {{ select_im_metadata_cols(im_name='im_invoice_metadata_pivot', rv_name='rv_stripe_mobile__invoices_metadata') }}
    from
        transactions trx
        left join {{ ref('im_charge') }} chg
            on chg.charge_id = trx.source_id
        left join {{ ref('im_invoice') }} inv
            on inv.charge_id = chg.charge_id
        left join {{ ref('im_subscription') }} sub
            on inv.subscription_id = sub.subscription_id
        left join {{ ref('im_invoice_item_pivot') }} iit
            on iit.invoice_id = inv.invoice_id
        left join {{ ref('im_invoice_metadata_pivot') }} met
            on met.invoice_id = inv.invoice_id
    where true
        and trx.reporting_category = 'charge'

),

final as (

    select
        balance_transaction_id,
        charge_id,
        refund_id,
        dispute_id,
        created_at,
        month_created_at,
        balance_transaction_description,
        reporting_category,
        gross_amount,
        fee_amount,
        net_amount,
        invoice_id,
        claim_id,
        subscription_id,
        plan_id,
        customer_id,
        charge_description,
        original_charge_at,
        business_line,
        charge_type,
        invoice_item_list,
        is_psp_no_claim,
        psp_combined_description,
        psp_clean_description,
        psp_date_candidate_str,
        psp_date_candidate,
        psp_parsed_dos_at,
        -- metadata
        {{ select_im_metadata_cols(im_name='im_balance_transaction', rv_name='rv_stripe_mobile__balance_transactions') }},
        {{ select_im_metadata_cols(im_name='im_charge', rv_name='rv_stripe_mobile__charges') }},
        {{ select_im_metadata_cols(im_name='im_refund', rv_name='rv_stripe_mobile__refunds', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_invoice', rv_name='rv_stripe_mobile__invoices') }},
        {{ select_im_metadata_cols(im_name='im_subscription', rv_name='rv_stripe_mobile__subscriptions') }},
        {{ select_im_metadata_cols(im_name='im_dispute', rv_name='rv_stripe_mobile__disputes', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_invoice_item_pivot', rv_name='rv_stripe_mobile__invoice_items') }},
        {{ select_im_metadata_cols(im_name='im_invoice_metadata_pivot', rv_name='rv_stripe_mobile__invoices_metadata') }}
    from charges

    union all

    select
        trx.balance_transaction_id,
        chg.charge_id,
        trx.source_id as refund_id,
        null as dispute_id,
        trx.created_at,
        trx.month_created_at,
        trx.balance_transaction_description,
        trx.reporting_category,
        trx.gross_amount,
        trx.fee_amount,
        trx.net_amount,
        chg.invoice_id,
        chg.claim_id,
        chg.subscription_id,
        chg.plan_id,
        chg.customer_id,
        chg.charge_description,
        chg.original_charge_at,
        coalesce(chg.business_line, 'unknown') as business_line,
        coalesce(chg.charge_type, 'unknown') as charge_type,
        chg.invoice_item_list,
        chg.is_psp_no_claim,
        chg.psp_combined_description,
        chg.psp_clean_description,
        chg.psp_date_candidate_str,
        chg.psp_date_candidate,
        chg.psp_parsed_dos_at,
        -- metadata
        {{ select_im_metadata_cols(im_name='trx.im_balance_transaction', rv_name='trx.rv_stripe_mobile__balance_transactions') }},
        {{ select_im_metadata_cols(im_name='chg.im_charge', rv_name='chg.rv_stripe_mobile__charges') }},
        {{ select_im_metadata_cols(im_name='ref.im_refund', rv_name='ref.rv_stripe_mobile__refunds') }},
        {{ select_im_metadata_cols(im_name='chg.im_invoice', rv_name='chg.rv_stripe_mobile__invoices') }},
        {{ select_im_metadata_cols(im_name='chg.im_subscription', rv_name='chg.rv_stripe_mobile__subscriptions') }},
        {{ select_im_metadata_cols(im_name='im_dispute', rv_name='rv_stripe_mobile__disputes', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_invoice_item_pivot', rv_name='chg.rv_stripe_mobile__invoice_items') }},
        {{ select_im_metadata_cols(im_name='im_invoice_metadata_pivot', rv_name='chg.rv_stripe_mobile__invoices_metadata') }}
    from transactions trx
    left join {{ ref('im_refund') }} ref 
        on trx.source_id = ref.refund_id 
    left join charges chg
        on ref.charge_id = chg.charge_id
    where true
        and trx.reporting_category in ('refund', 'refund_failure') 

    union all

    select
        trx.balance_transaction_id,
        chg.charge_id,
        null as refund_id,
        trx.source_id as dispute_id,
        trx.created_at,
        trx.month_created_at,
        trx.balance_transaction_description,
        trx.reporting_category,
        trx.gross_amount,
        trx.fee_amount,
        trx.net_amount,
        chg.invoice_id,
        chg.claim_id,
        chg.subscription_id,
        chg.plan_id,
        chg.customer_id,
        chg.charge_description,
        chg.original_charge_at,
        coalesce(chg.business_line, 'unknown') as business_line,
        coalesce(chg.charge_type, 'unknown') as charge_type,
        chg.invoice_item_list,
        chg.is_psp_no_claim,
        chg.psp_combined_description,
        chg.psp_clean_description,
        chg.psp_date_candidate_str,
        chg.psp_date_candidate,
        chg.psp_parsed_dos_at,
        -- metadata
        {{ select_im_metadata_cols(im_name='trx.im_balance_transaction', rv_name='trx.rv_stripe_mobile__balance_transactions') }},
        {{ select_im_metadata_cols(im_name='chg.im_charge', rv_name='chg.rv_stripe_mobile__charges') }},
        {{ select_im_metadata_cols(im_name='im_refund', rv_name='rv_stripe_mobile__refunds', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='chg.im_invoice', rv_name='chg.rv_stripe_mobile__invoices') }},
        {{ select_im_metadata_cols(im_name='chg.im_subscription', rv_name='chg.rv_stripe_mobile__subscriptions') }},
        {{ select_im_metadata_cols(im_name='dis.im_dispute', rv_name='dis.rv_stripe_mobile__disputes') }},
        {{ select_im_metadata_cols(im_name='im_invoice_item_pivot', rv_name='chg.rv_stripe_mobile__invoice_items') }},
        {{ select_im_metadata_cols(im_name='im_invoice_metadata_pivot', rv_name='chg.rv_stripe_mobile__invoices_metadata') }}
    from transactions trx
    left join {{ ref('im_dispute') }} dis
        on trx.source_id = dis.dispute_id
    left join charges chg
        on dis.charge_id = chg.charge_id
    where true
        and trx.reporting_category in ('dispute', 'dispute_reversal')

    union all

    select
        trx.balance_transaction_id,
        null as charge_id,
        null as refund_id,
        null as dispute_id,
        trx.created_at,
        trx.month_created_at,
        trx.balance_transaction_description,
        trx.reporting_category,
        trx.gross_amount,
        trx.fee_amount,
        trx.net_amount,
        null as invoice_id,
        null as claim_id,
        null as subscription_id,
        null as plan_id,
        null as customer_id,
        null as charge_description,
        null as original_charge_at,
        'unknown' as business_line,
        'unknown' as charge_type,
        null as invoice_item_list,
        null as is_psp_no_claim,
        null as psp_combined_description,
        null as psp_clean_description,
        null as psp_date_candidate_str,
        null as psp_date_candidate,
        null as psp_parsed_dos_at,
        -- metadata
        {{ select_im_metadata_cols(im_name='trx.im_balance_transaction', rv_name='trx.rv_stripe_mobile__balance_transactions') }},
        {{ select_im_metadata_cols(im_name='im_charge', rv_name='rv_stripe_mobile__charges', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_refund', rv_name='rv_stripe_mobile__refunds', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_invoice', rv_name='rv_stripe_mobile__invoices', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_subscription', rv_name='rv_stripe_mobile__subscriptions', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_dispute', rv_name='rv_stripe_mobile__disputes', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_invoice_item_pivot', rv_name='rv_stripe_mobile__invoice_items', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_invoice_metadata_pivot', rv_name='rv_stripe_mobile__invoices_metadata', use_null_value=True) }}
    from transactions trx
    where true
        and trx.reporting_category in ('other_adjustment')

    union all

    select
        trx.balance_transaction_id,
        null as charge_id,
        null as refund_id,
        null as dispute_id,
        trx.created_at,
        trx.month_created_at,
        trx.balance_transaction_description,
        'platform_fee' as reporting_category,
        0.0 as gross_amount,
        trx.gross_amount * -1.0 as fee_amount,
        trx.net_amount,
        null as invoice_id,
        null as claim_id,
        null as subscription_id,
        null as plan_id,
        null as customer_id,
        null as charge_description,
        null as original_charge_at,
        'platform_fee' as business_line,
        'platform_fee' as charge_type,
        null as invoice_item_list,
        null as is_psp_no_claim,
        null as psp_combined_description,
        null as psp_clean_description,
        null as psp_date_candidate_str,
        null as psp_date_candidate,
        null as psp_parsed_dos_at,
        -- metadata
        {{ select_im_metadata_cols(im_name='trx.im_balance_transaction', rv_name='trx.rv_stripe_mobile__balance_transactions') }},
        {{ select_im_metadata_cols(im_name='im_charge', rv_name='rv_stripe_mobile__charges', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_refund', rv_name='rv_stripe_mobile__refunds', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_invoice', rv_name='rv_stripe_mobile__invoices', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_subscription', rv_name='rv_stripe_mobile__subscriptions', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_dispute', rv_name='rv_stripe_mobile__disputes', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_invoice_item_pivot', rv_name='rv_stripe_mobile__invoice_items', use_null_value=True) }},
        {{ select_im_metadata_cols(im_name='im_invoice_metadata_pivot', rv_name='rv_stripe_mobile__invoices_metadata', use_null_value=True) }}
    from transactions trx
    where true
        and trx.reporting_category in ('fee')

)

select 
    '{{ jira_task_key }}' as jira_task_key,
    {{ 
        build_hash_value(
            value=build_hash_diff(
                        cols=[
                                'im_balance_transaction_hk', 
                                'im_charge_hk', 
                                'im_refund_hk', 
                                'im_invoice_hk', 
                                'im_subscription_hk', 
                                'im_dispute_hk',
                                'im_invoice_item_pivot_hk',
                                'im_invoice_metadata_pivot_hk'
                            ]
                    ),
            alias='mr_balance_transaction_hk'
        )
    }},
    *
from final
