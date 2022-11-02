{# 
    This is a template to follow for the creation of br_ concept models.
    See br_invoice for a real-world example.
 #}
 
{{ build_br_concept_model(concept_model_name=this.name, base_model_name='br_schema_name__table_name', jira_task_key='PLATFORM-DEFAULT') }}
