

with windowed_requests_approval as (
select
    max(created_at) OVER (PARTITION BY request_id) as final_date,
    min(created_at) OVER (PARTITION BY REQUEST_ID) first_date,
    first_value(approval_id) OVER (PARTITION BY REQUEST_ID ORDER BY created_at) first_approval_id,
    last_value(approval_id) OVER (PARTITION BY REQUEST_ID ORDER BY created_at) last_approval_id,
    first_value(action) OVER (PARTITION BY REQUEST_ID ORDER BY created_at) first_action,
    last_value(action) OVER (PARTITION BY REQUEST_ID ORDER BY created_at) last_action,
    ra.*
from orbit_mds_dev.raw.requests_approvals ra
)
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
where first_date = created_at