select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select loan_key
from AUTOCHECK.MART.dim_loans
where loan_key is null



      
    ) dbt_internal_test