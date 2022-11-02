{% set alias = this.name.replace('br_', '') %}

with 

rv_base as (
    
    select
        -- business key
        id as charge_id,

        -- fks
        application_fee_id,
        application_id,
        balance_transaction_id,
        card_customer_id,
        card_recipient_id,
        customer_id,
        {{ user_id_from_customer_id('customer_id') }},
        destination_id,
        dispute_id,
        invoice_id,
        merchant_id,
        on_behalf_of_id,
        order_id,
        outcome_rule_id,
        payment_intent,
        payment_method_id,
        source_id,
        source_transfer_id,
        transfer_id,

        -- misc
        calculated_statement_descriptor,
        captured as is_captured,
        card_address_city,
        card_address_country,
        card_address_state,
        card_brand,
        card_country,
        card_currency,
        card_cvc_check,
        card_default_for_currency as is_card_default_for_currency,
        card_fingerprint,
        card_funding,
        card_iin,
        card_tokenization_method,
        currency,
        description as charge_description,
        failure_code,
        failure_message,
        outcome_network_status,
        outcome_reason,
        outcome_risk_level,
        outcome_risk_score,
        outcome_seller_message,
        outcome_type,
        paid as is_paid,
        payment_method_type,
        receipt_number,
        refunded as is_refunded,
        shipping_address_city,
        shipping_address_country,
        shipping_address_state,
        statement_descriptor,
        statement_descriptor_suffix,
        status as charge_status,
        transfer_group,
        
        -- metrics
        {{ decimalize(column='amount') }},
        {{ decimalize(column='amount_refunded') }},
        
        -- dates
        captured_at as captured_at,
        created as created_at,

        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    where true

),

{{ build_br_base_model(alias) }}
