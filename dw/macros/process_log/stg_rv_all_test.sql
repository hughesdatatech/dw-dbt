{% macro stg_rv_all_test(test_type) %}

with

{% for source_schema in var('stg_schema_config') %}

    {% for source_schema__table in var('stg_' + source_schema + '__model_config') %}

        {%- set source_schema = get_source_schema(source_schema__table) -%}
        {%- set tgt_table_name = 'rv_' + source_schema__table -%}
        {%- set stg_table_name = 'stg_' + source_schema__table -%}
        {%- set unique_key = get_model_config(stg_table_name, 'unique_key') -%}
        {%- set snapshot_strategy = get_schema_config(source_schema, 'snapshot_strategy') -%}
        {%- set snapshot_updated_at_col = get_schema_config(source_schema, 'snapshot_updated_at_col') -%}
        {%- set cols_for_uniqueness_recon = [] -%}
        {%- set cols_for_uniqueness_dupe = [] -%}

        {%- if snapshot_strategy == 'timestamp' -%}
            {%- set cols_for_uniqueness_recon = unique_key -%}
            {%- do cols_for_uniqueness_recon.append(snapshot_updated_at_col) -%}
            {%- set cols_for_uniqueness_dupe = cols_for_uniqueness_recon.copy() -%}
        {%- endif -%}

        {%- if snapshot_strategy == 'check' -%}
            {% set cols_for_uniqueness_recon = unique_key + ['rv_' + source_schema__table + '_hd'] -%}
            {%- set cols_for_uniqueness_dupe = cols_for_uniqueness_recon.copy() -%}
            {%- do cols_for_uniqueness_dupe.append('rv_' + source_schema__table + '_loaded_at') -%}
        {%- endif %}

        {% if test_type == 'recon' -%}
            test_{{ source_schema__table }} as (
                {{
                    stg_rv_recon_test(
                        tgt_table_name=tgt_table_name,
                        stg_table_name=stg_table_name,
                        cols_for_uniqueness=cols_for_uniqueness_recon,
                        strategy=snapshot_strategy,
                        test_number = loop.index0
                    )
                }}
            )
        {%- elif test_type == 'dupe' -%}
            test_{{ source_schema__table }} as (
                {{
                    stg_rv_dupe_test(
                        tgt_table_name=tgt_table_name,
                        stg_table_name=stg_table_name,
                        cols_for_uniqueness=cols_for_uniqueness_dupe,
                        test_number = loop.index0
                    )
                }}
            )
        {%- endif %}

    {% if not loop.last -%},{%- endif %}

    {% endfor %}

{% if not loop.last -%},{%- endif %}

{% endfor %}

{% for source_schema in var('stg_schema_config') %}

    {% for source_schema__table in var('stg_' + source_schema + '__model_config') %}
        
        select * 
        from test_{{ source_schema__table }}
    {% if not loop.last -%}union all{%- endif %}
    
    {% endfor %}

{% if not loop.last -%}union all{%- endif %}

{% endfor %}

{% endmacro %}
