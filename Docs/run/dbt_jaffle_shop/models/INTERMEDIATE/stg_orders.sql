
  create or replace   view JAFFLE_SHOP.INTERMEDIATE.stg_orders
  
   as (
    SELECT 
id As order_id,
customer As customer_id,
store_id,
subtotal,
tax_paid,
order_total,
TRY_CAST(ordered_at As TIMESTAMP) As ordered_date,
_AIRBYTE_EXTRACTED_AT
FROM JAFFLE_SHOP.RAW.raw_orders
  );

