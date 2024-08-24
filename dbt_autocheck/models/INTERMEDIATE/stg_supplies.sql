
SELECT
ID As supplies_id,
sku As product_id,
cost,
name As supplies_name,
perishable,
_AIRBYTE_EXTRACTED_AT
FROM {{ source('jaffle_shop', 'raw_supplies') }}