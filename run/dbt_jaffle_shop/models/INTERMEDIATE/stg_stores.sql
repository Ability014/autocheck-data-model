
  create or replace   view JAFFLE_SHOP.INTERMEDIATE.stg_stores
  
   as (
    SELECT
ID as store_id,
NAME As store_name,
tax_rate,
CAST(opened_at as DATE) As opened_at,
_AIRBYTE_EXTRACTED_AT
FROM JAFFLE_SHOP.RAW.raw_stores
  );

