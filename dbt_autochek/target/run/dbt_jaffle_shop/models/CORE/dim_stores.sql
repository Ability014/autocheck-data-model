
  
    

        create or replace  table JAFFLE_SHOP.CORE.dim_stores
         as
        (SELECT 
md5(cast(coalesce(cast(store_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as store_key,
*
FROM JAFFLE_SHOP.INTERMEDIATE.stg_stores
        );
      
  