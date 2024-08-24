select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select orders_key
from JAFFLE_SHOP.CORE.fact_orders
where orders_key is null



      
    ) dbt_internal_test