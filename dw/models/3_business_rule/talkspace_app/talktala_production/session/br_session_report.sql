with 

final as (

    {{ build_br_concept_model(concept_model_name=this.name, base_model_name='br_talktala_production__session_reports', jira_task_key='PLATFORM-1955') }}

)

select
    *,
    -- calculated columns
    case 
        when completed_at is not null
        then True
        else False
    end is_complete
from final
    