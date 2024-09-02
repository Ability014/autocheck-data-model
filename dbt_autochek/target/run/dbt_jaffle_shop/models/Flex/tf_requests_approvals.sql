-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into orbit_mds_dev.core.tf_requests_approvals as DBT_INTERNAL_DEST
        using orbit_mds_dev.core.tf_requests_approvals__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.first_approval_id = DBT_INTERNAL_DEST.first_approval_id
            )

    
    when matched then update set
        last_approval_id = DBT_INTERNAL_SOURCE.last_approval_id,last_action = DBT_INTERNAL_SOURCE.last_action,final_date = DBT_INTERNAL_SOURCE.final_date,time_to_final_decision = DBT_INTERNAL_SOURCE.time_to_final_decision
    

    when not matched then insert
        ("FIRST_APPROVAL_ID", "LAST_APPROVAL_ID", "REQUEST_ID", "ORG_ID", "AMOUNT", "FIRST_ACTION", "LAST_ACTION", "FIRST_DATE", "FINAL_DATE", "TIME_TO_FINAL_DECISION", "USER_ID")
    values
        ("FIRST_APPROVAL_ID", "LAST_APPROVAL_ID", "REQUEST_ID", "ORG_ID", "AMOUNT", "FIRST_ACTION", "LAST_ACTION", "FIRST_DATE", "FINAL_DATE", "TIME_TO_FINAL_DECISION", "USER_ID")

;
    commit;