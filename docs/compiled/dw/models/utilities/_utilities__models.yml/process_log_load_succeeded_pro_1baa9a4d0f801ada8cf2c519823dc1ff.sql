select
    *
from 
    dw_dev.dbt_steve.process_log_stg_rv_dupe_test
where true
    and key_dupes != 0
    and inserted_at = (
        select max(inserted_at) from dw_dev.dbt_steve.process_log_stg_rv_dupe_test
    )