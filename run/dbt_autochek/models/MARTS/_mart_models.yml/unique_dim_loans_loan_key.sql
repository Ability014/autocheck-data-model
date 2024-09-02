select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    loan_key as unique_field,
    count(*) as n_records

from AUTOCHECK.MART.dim_loans
where loan_key is not null
group by loan_key
having count(*) > 1



      
    ) dbt_internal_test