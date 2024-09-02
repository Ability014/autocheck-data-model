select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    schedule_payments_key as unique_field,
    count(*) as n_records

from AUTOCHECK.MART.fct_repayments
where schedule_payments_key is not null
group by schedule_payments_key
having count(*) > 1



      
    ) dbt_internal_test