SELECT
ID As item_id,
sku As product_id,
order_id,
_AIRBYTE_EXTRACTED_AT
FROM JAFFLE_SHOP.RAW.raw_items