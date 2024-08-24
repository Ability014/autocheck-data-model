{{
    config(
        materialized='incremental',
        unique_key = 'first_approval_id',
        incremetal_strategy = 'merge',
        merge_update_columns = ['last_approval_id', 'last_action', 'final_date', 'time_to_final_decision']
    )
}}

select
    first_approval_id, 
    last_approval_id,
    request_id,
    org_id,
    amount,
    first_action,
    last_action,
    first_date,
    final_date,
    datediff(minute, first_date, final_date) as time_to_final_decision,
    user_id
from {{ ref('stg_request_approvals') }}
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  where final_date > (select coalesce(max(final_date), '1900-01-01') from {{ this }})

{% endif %}