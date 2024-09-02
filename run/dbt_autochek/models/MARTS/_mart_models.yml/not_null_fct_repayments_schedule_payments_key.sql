select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select schedule_payments_key
from AUTOCHECK.MART.fct_repayments
where schedule_payments_key is null



      
    ) dbt_internal_test