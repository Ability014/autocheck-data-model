
  create or replace   view JAFFLE_SHOP.INTERMEDIATE.stg_products
  
   as (
    SELECT
SKU As product_id,
NAME As product_name,
TYPE As product_type,
PRICE As unit_price,
DESCRIPTION As description,
_AIRBYTE_EXTRACTED_AT
FROM JAFFLE_SHOP.RAW.raw_products
  );

