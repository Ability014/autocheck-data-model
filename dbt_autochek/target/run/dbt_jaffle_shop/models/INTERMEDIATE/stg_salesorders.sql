
  create or replace   view JAFFLE_SHOP.INTERMEDIATE.stg_salesorders
  
   as (
    SELECT 
ro.id As order_id,
ri.id As item_id,
ro.customer As customer_id,
ro.store_id,
ri.sku As product_id,
rp.price As subtotal,
(rp.price * tax_rate) As tax_paid,
rp.price + (rp.price * tax_rate) As order_total,
TRY_CAST(ordered_at As TIMESTAMP) As ordered_date,
ro._AIRBYTE_EXTRACTED_AT
FROM JAFFLE_SHOP.RAW.raw_orders as ro
JOIN JAFFLE_SHOP.RAW.raw_items as ri
ON ro.id=ri.order_id
LEFT JOIN JAFFLE_SHOP.RAW.raw_products as rp
ON ri.sku=rp.sku
LEFT JOIN JAFFLE_SHOP.RAW.raw_stores as rs
ON ro.store_id=rs.id
  );

