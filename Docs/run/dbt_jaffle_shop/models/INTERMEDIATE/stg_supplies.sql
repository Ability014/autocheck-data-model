
  create or replace   view JAFFLE_SHOP.INTERMEDIATE.stg_supplies
  
   as (
    SELECT
ID As supplies_id,
sku As product_id,
cost,
name As supplies_name,
perishable,
_AIRBYTE_EXTRACTED_AT
FROM JAFFLE_SHOP.RAW.raw_supplies
  );

