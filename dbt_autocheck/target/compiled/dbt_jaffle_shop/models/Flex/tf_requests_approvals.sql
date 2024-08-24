

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
from orbit_mds_dev.core.stg_request_approvals


  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  where final_date > (select coalesce(max(final_date), '1900-01-01') from orbit_mds_dev.core.tf_requests_approvals)

