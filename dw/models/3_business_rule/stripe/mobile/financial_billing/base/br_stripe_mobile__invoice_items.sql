{% set alias = this.name.replace('br_', '') %}
with 

rv_base as (

    select
         -- business key
        id as invoice_item_id,

        -- fks
        customer_id,
        {{ user_id_from_customer_id('customer_id') }},
        invoice_id,
        merchant_id,
        plan_id,
        price_id,
        subscription_id,

        -- misc
        currency,
        {{ coalesce_column(column='"description"', alias='invoice_item_description') }},
        discountable as is_discountable,
        proration as is_proration,

        -- metrics
        {{ decimalize(column='amount') }},
        quantity,
        {{ decimalize(column='unit_amount') }},

        -- dates
        "date" as item_dated_at,
        period_end as period_ended_at,
        period_start as period_started_at,

        -- required, do not modify
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}
    from {{ ref('rv_' + alias) }} as rv
    where true

),

{{ build_br_base_model(alias) }}
