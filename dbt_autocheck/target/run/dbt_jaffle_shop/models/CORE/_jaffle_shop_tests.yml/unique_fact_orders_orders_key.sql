select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    orders_key as unique_field,
    count(*) as n_records

from JAFFLE_SHOP.CORE.fact_orders
where orders_key is not null
group by orders_key
having count(*) > 1



      
    ) dbt_internal_test