{% snapshot rv_talktala_production__session_reports %}

    {{ build_snapshot_model(this.name) }}

{% endsnapshot %}

{% snapshot rv_talktala_production__insurance_payers %}

    {{ build_snapshot_model(this.name) }}

{% endsnapshot %}

{% snapshot rv_talktala_production__payment_transactions %}

    {{ build_snapshot_model(this.name) }}

{% endsnapshot %}

{% snapshot rv_talktala_production_private_talks %}

{{
    config(
      tags=['hold'],
      target_schema=target.get('schema'),
      unique_key='id',
      strategy='timestamp',
      updated_at='_dms_cdc_ts',
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_talktala_production__private_talks') }}

{% endsnapshot %}

{% snapshot rv_talktala_production_users %}

{{
    config(
      tags=['hold'],
      target_schema=target.get('schema'),
      unique_key='id',
      strategy='timestamp',
      updated_at='_dms_cdc_ts',
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_talktala_production__users') }}

{% endsnapshot %}

{% snapshot rv_talktala_production_first_purchase %}

{{
    config(
      tags=['hold'],
      target_schema=target.get('schema'),
      unique_key='private_talk_id',
      strategy='timestamp',
      updated_at='_dms_cdc_ts',
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_talktala_production__first_purchase') }}

{% endsnapshot %}

{% snapshot rv_talktala_production_plan_type_payment_type_mapping %}

{{
    config(
      tags=['hold'],
      target_schema=target.get('schema'),
      unique_key='private_talks_payment_type_id',
      strategy='timestamp',
      updated_at='_dms_cdc_ts',
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_talktala_production__plan_type_payment_type_mapping') }}

{% endsnapshot %}

{% snapshot rv_talktala_production_plan %}

{{
    config(
      tags=['hold'],
      target_schema=target.get('schema'),
      unique_key='plan_id',
      strategy='timestamp',
      updated_at='_dms_cdc_ts',
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_talktala_production__plan') }}

{% endsnapshot %}

{% snapshot rv_talktala_production_plan_details %}

{{
    config(
      tags=['hold'],
      target_schema=target.get('schema'),
      unique_key='plan_id',
      strategy='timestamp',
      updated_at='_dms_cdc_ts',
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_talktala_production__plan_details') }}

{% endsnapshot %}

{% snapshot rv_talktala_production_plan_credit_settings %}

{{
    config(
      tags=['hold'],
      target_schema=target.get('schema'),
      unique_key='id',
      strategy='timestamp',
      updated_at='_dms_cdc_ts',
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_talktala_production__plan_credit_settings') }}

{% endsnapshot %}

{% snapshot rv_talktala_production_auto_cancellation_settings %}

{{
    config(
      tags=['hold'],
      target_schema=target.get('schema'),
      unique_key='id',
      strategy='timestamp',
      updated_at='_dms_cdc_ts',
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_talktala_production__auto_cancellation_settings') }}

{% endsnapshot %}

{% snapshot rv_talktala_production_plan_to_plan_group %}

{{
    config(
      tags=['hold'],
      target_schema=target.get('schema'),
      unique_key='id',
      strategy='timestamp',
      updated_at='_dms_cdc_ts',
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_talktala_production__plan_to_plan_group') }}

{% endsnapshot %}

{% snapshot rv_talktala_production_payment_types %}

{{
    config(
      tags=['hold'],
      target_schema=target.get('schema'),
      unique_key='id',
      strategy='timestamp',
      updated_at='_dms_cdc_ts',
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_talktala_production__payment_types') }}

{% endsnapshot %}

{% snapshot rv_talktala_production_payment_type_policies %}

{{
    config(
      tags=['hold'],
      target_schema=target.get('schema'),
      unique_key='id',
      strategy='timestamp',
      updated_at='_dms_cdc_ts',
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_talktala_production__payment_type_policies') }}

{% endsnapshot %}

{% snapshot rv_talktala_production_partner %}

{{
    config(
      tags=['hold'],
      target_schema=target.get('schema'),
      unique_key='id',
      strategy='timestamp',
      updated_at='_dms_cdc_ts',
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_talktala_production__partner') }}

{% endsnapshot %}
