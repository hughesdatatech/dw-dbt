{# 
    This is a template to follow for the creation of br_ base models.
    See br_stripe_mobile__invoices for a real-world example.
 #}

{% set alias = this.name.replace('br_', '') %} -- Do not modify

with 

rv_base as (

    select

        -------------------------------------------------------
        -- Begin: Customize the following sections accordingly.
        -------------------------------------------------------

        --------------------------------
        -- Define the business key here.
        --------------------------------
        -- Rename according to business concept names
        -- e.g.
        id as concept_name_id,
        
        -----------------------------------------------
        -- List any foreign keys to other objects here.
        -----------------------------------------------
        -- Rename according to business concept names
        -- e.g.
        -- user_id as client_id,
        -- therapist_id as provider_id,

        --------------------------------------
        -- List any miscellaneous fields here.
        --------------------------------------
        -- Rename booleans according to the style guide
        -- e.g. 
        -- attempted as is_attempted,
        -- paid as is_paid,

        -------------------------
        -- List any metrics here.
        -------------------------
        -- Utilize the {{ decimalize }} macro to convert raw numbers to decimals
        -- e.g.
        -- {{ decimalize(column='amount') }},
        
        -----------------------
        -- List any dates here.
        -----------------------
        -- Rename according to the style guide
        -- e.g.
        -- created as created_at,
        -- invoiced as invoiced_at,
        
        -----------------------------------------------------
        -- End: Customize the following sections accordingly.
        -----------------------------------------------------

        ------------------------------------------
        -- Do not modify anything below this line.
        ------------------------------------------ 
        
        rv_{{ alias }}_hk,
        rv_{{ alias }}_loaded_at,
        {{ build_dbt_metadata_cols('rv_' + alias) }}  
    from {{ ref('rv_' + alias) }}
    where true

),

{{ build_br_base_model(alias) }}
