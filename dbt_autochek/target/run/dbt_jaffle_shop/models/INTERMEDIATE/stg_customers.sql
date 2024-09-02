
  create or replace   view JAFFLE_SHOP.INTERMEDIATE.stg_customers
  
   as (
    SELECT 
ID As customer_id,
NAME As customer_name,
_AIRBYTE_EXTRACTED_AT
FROM JAFFLE_SHOP.RAW.raw_customers
  );

