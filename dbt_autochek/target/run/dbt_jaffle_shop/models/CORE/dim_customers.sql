
  
    

        create or replace  table JAFFLE_SHOP.CORE.dim_customers
         as
        (SELECT 
md5(cast(coalesce(cast(customer_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as customer_key,
*
FROM JAFFLE_SHOP.INTERMEDIATE.stg_customers
        );
      
  