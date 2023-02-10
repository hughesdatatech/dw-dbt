select
    *
from 
    dw_dev.dbt_steve.process_log_stg_rv_recon_test
where true
    and unique_key_tgt_not_loaded != 0
    and inserted_at = (
        select max(inserted_at) from dw_dev.dbt_steve.process_log_stg_rv_recon_test
    )