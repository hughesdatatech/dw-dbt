{# 
  ####################################
  Snapshot example for all strategies                                 
  #################################### 
#}

{#

{% snapshot rv_[schema_name__table_name] %}

  {{ build_snapshot_model(this.name) }}

{% endsnapshot %}

#}

{% snapshot rv_schema_name__table_name %}

{{
    config(
      target_schema=target.get('schema'),
      unique_key='rv_schema_name__table_name_hk',
      strategy='timestamp',
      updated_at='_dms_cdc_ts',
    )
}}

select
    'default' as rv_schema_name__table_name_tenant_key,
    'default'::varbinary(64) as rv_schema_name__table_name_hk,
    'default' as rv_schema_name__table_name_rec_source,
    'default' as rv_schema_name__table_name_job_id,
    'default' as rv_schema_name__table_name_job_user_id,
    'default' as rv_schema_name__table_name_jira_task_key,
    sysdate() as rv_schema_name__table_name_extracted_at,
    sysdate() as rv_schema_name__table_name_loaded_at,
    1 as id,
    sysdate() as _dms_cdc_ts

{% endsnapshot %}
