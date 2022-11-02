{% set alias = this.name.replace('br_', '') %}

with 

rv_base as (

    select
        -- business key
        id as invoice_id,
        
        -- fks
        charge_id,
        customer_id,
        {{ user_id_from_customer_id('customer_id') }},
        discount_coupon_id,
        discount_customer_id,
        discount_subscription
        merchant_id,
        on_behalf_of_id,
        subscription_id,
        transfer_data_destination_id,

        -- misc
        attempted as is_attempted,
        auto_advance as is_auto_advance,
        billing_reason,
        collection_method,
        currency,
        "description" as invoice_description,
        paid as is_paid,
        paid_out_of_band as is_paid_out_of_band,
        "number" as invoice_number,
        receipt_number,
        statement_descriptor,
        "status" as invoice_status,

        -- metrics
        {{ decimalize(column='amount_due') }},
        {{ decimalize(column='amount_paid') }},
        {{ decimalize(column='amount_remaining') }},
        {{ decimalize(column='application_fee') }},
        attempt_count,
        {{ decimalize(column='ending_balance') }},
        {{ decimalize(column='starting_balance') }},
        {{ decimalize(column='subtotal') }},
        {{ decimalize(column='tax') }},
        tax_percent,
        {{ decimalize(column='total') }},
        {{ decimalize(column='transfer_data_amount') }},

        -- dates
        "date" as invoiced_at,
        discount_end as discount_ended_at,
        discount_start as discount_started_at,
        due_date as due_at,
        next_payment_attempt as next_payment_attempted_at,
        period_end as period_ended_at,
        period_start as period_started_at,
        status_transitions_finalized_at,
        status_transitions_marked_uncollectible_at,
        status_transitions_paid_at,
        status_transitions_voided_at,
        subscription_proration_date as subscription_prorated_at,
        webhooks_delivered_at,

        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    where true

),

{{ build_br_base_model(alias) }}
