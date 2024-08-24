{{
    config(
        materialized='incremental',
        unique_key = 'first_approval_id',
        incremetal_strategy = 'merge',
        merge_update_columns = ['last_approval_id', 'last_action', 'final_date', 'time_to_final_decision']
    )
}}

with windowed_requests_approval as (
select
    max(created_at) OVER (PARTITION BY request_id) as final_date,
    min(created_at) OVER (PARTITION BY REQUEST_ID) first_date,
    first_value(approval_id) OVER (PARTITION BY REQUEST_ID ORDER BY created_at) first_approval_id,
    last_value(approval_id) OVER (PARTITION BY REQUEST_ID ORDER BY created_at) last_approval_id,
    first_value(action) OVER (PARTITION BY REQUEST_ID ORDER BY created_at) first_action,
    last_value(action) OVER (PARTITION BY REQUEST_ID ORDER BY created_at) last_action,
    ra.*
from {{ source('flex', 'requests_approvals') }} ra
),
filtered_requests_approval as (
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
from windowed_requests_approval
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  where final_date > (select coalesce(max(final_date), '1900-01-01') from {{ this }}) and first_date = created_at

{% endif %}
)
select
    *
from filtered_requests_approval
--where first_date = created_at